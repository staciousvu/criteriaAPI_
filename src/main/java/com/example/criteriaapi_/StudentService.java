package com.example.criteriaapi_;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.criteria.*;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class StudentService {
    @PersistenceContext
    private EntityManager entityManager;

    public List<Student> findStudentsByAgeRange(Integer minAge,Integer maxAge){
        CriteriaBuilder criteriaBuilder= entityManager.getCriteriaBuilder();
        CriteriaQuery<Student> criteriaQuery=criteriaBuilder.createQuery(Student.class);
        Root<Student> root=criteriaQuery.from(Student.class);
        Predicate ageGTE=criteriaBuilder.greaterThanOrEqualTo(root.get("age"),minAge);
        Predicate ageLTE= criteriaBuilder.lessThanOrEqualTo(root.get("age"),maxAge);
        criteriaQuery.where(criteriaBuilder.and(ageLTE,ageGTE));
        return entityManager.createQuery(criteriaQuery).getResultList();
    }
    public List<Student> findStudentsByNameKeyword(String keyword){
        CriteriaBuilder criteriaBuilder= entityManager.getCriteriaBuilder();
        CriteriaQuery<Student> criteriaQuery= criteriaBuilder.createQuery(Student.class);
        Root<Student> root=criteriaQuery.from(Student.class);
        Predicate likeKeyword=criteriaBuilder.like(root.get("name"),"%"+keyword+"%");
        criteriaQuery.where(criteriaBuilder.and(likeKeyword));
        return entityManager.createQuery(criteriaQuery).getResultList();
    }

    public List<Student> findAllStudentsOrderByNameAndAge(){
        CriteriaBuilder criteriaBuilder= entityManager.getCriteriaBuilder();
        CriteriaQuery<Student> criteriaQuery=criteriaBuilder.createQuery(Student.class);
        Root<Student> root=criteriaQuery.from(Student.class);
        criteriaQuery.orderBy(criteriaBuilder.asc(root.get("name")),
                criteriaBuilder.asc(root.get("age")));
        return entityManager.createQuery(criteriaQuery).getResultList();
    }
    public List<Student> findStudentsWithMaxAge(){
        CriteriaBuilder criteriaBuilder= entityManager.getCriteriaBuilder();

        CriteriaQuery<Integer> ageQuery= criteriaBuilder.createQuery(Integer.class);
        Root<Student> studentRoot=ageQuery.from(Student.class);
        ageQuery.select(criteriaBuilder.max(studentRoot.get("age")));
        Integer maxAge=entityManager.createQuery(ageQuery).getSingleResult();

        CriteriaQuery<Student> criteriaQuery= criteriaBuilder.createQuery(Student.class);
        studentRoot=criteriaQuery.from(Student.class);
        criteriaQuery.where(criteriaBuilder.equal(studentRoot.get("age"),maxAge));
        return entityManager.createQuery(criteriaQuery).getResultList();
    }
    public Map<Integer,Integer> countStudentsByAge(){
        CriteriaBuilder criteriaBuilder= entityManager.getCriteriaBuilder();

        CriteriaQuery<Object[]> criteriaQuery=criteriaBuilder.createQuery(Object[].class);
        Root<Student> root=criteriaQuery.from(Student.class);
        criteriaQuery.multiselect(root.get("age"),criteriaBuilder.count(root.get("id")).alias("count"));
        criteriaQuery.groupBy(root.get("age"));
        List<Object[]> resultList=entityManager.createQuery(criteriaQuery).getResultList();
        Map<Integer,Integer> ageCountMap=new HashMap<>();
        return resultList.stream().collect(Collectors.toMap(
                row -> (Integer) row[0],
                row -> (Integer) row[1]
        ));
    }
    public List<String> findDistinctAddresses(){
        CriteriaBuilder criteriaBuilder= entityManager.getCriteriaBuilder();

        CriteriaQuery<String> criteriaQuery= criteriaBuilder.createQuery(String.class);
        Root<Student> root=criteriaQuery.from(Student.class);
        criteriaQuery.select(root.get("address")).distinct(true);
        return entityManager.createQuery(criteriaQuery).getResultList();
    }
    public Student findYoungestStudentByAddress(String address){
        CriteriaBuilder criteriaBuilder= entityManager.getCriteriaBuilder();

        CriteriaQuery<Integer> youngestAgeQuery= criteriaBuilder.createQuery(Integer.class);
        Root<Student> root=youngestAgeQuery.from(Student.class);
        youngestAgeQuery.select(criteriaBuilder.min(root.get("age"))).where(criteriaBuilder.
                equal(root.get("address"),address));
        Integer youngestAge=entityManager.createQuery(youngestAgeQuery).getSingleResult();

        CriteriaQuery<Student> criteriaQuery=criteriaBuilder.createQuery(Student.class);
        root=criteriaQuery.from(Student.class);
        criteriaQuery.where(criteriaBuilder.equal(root.get("age"),youngestAge));
        return entityManager.createQuery(criteriaQuery).getSingleResult();
    }
    public List<Student> findStudentsWithInvalidPhone(){
        CriteriaBuilder criteriaBuilder= entityManager.getCriteriaBuilder();
        CriteriaQuery<Student> criteriaQuery=criteriaBuilder.createQuery(Student.class);
        Root<Student> root=criteriaQuery.from(Student.class);
        Predicate validLength=criteriaBuilder.notEqual(criteriaBuilder.length(root.get("phone")),10);
        Predicate validNumber=criteriaBuilder.notLike(root.get("phone"),"\\d{10}");
        criteriaQuery.where(validNumber,validLength);
        return entityManager.createQuery(criteriaQuery).getResultList();
    }
    public List<Student> findDuplicateStudentsByNameAndAddress(){
        CriteriaBuilder criteriaBuilder= entityManager.getCriteriaBuilder();

        CriteriaQuery<Object[]> dupplicatedStudentQuery=criteriaBuilder.createQuery(Object[].class);
        Root<Student> root= dupplicatedStudentQuery.from(Student.class);
        dupplicatedStudentQuery.multiselect(root.get("name"),root.get("address"))
                .groupBy(root.get("name"),root.get("address"))
                .having(criteriaBuilder.greaterThan(criteriaBuilder
                        .count(root), 1L));
        List<Object[]> dupplicates=entityManager.createQuery(dupplicatedStudentQuery).getResultList();
        CriteriaQuery<Student> criteriaQuery= criteriaBuilder.createQuery(Student.class);
        root=criteriaQuery.from(Student.class);
        List<Predicate> predicates=new ArrayList<>();
        for (Object[] objects:dupplicates){
            String name=(String) objects[0];
            String address=(String) objects[1];
            Predicate nameCondition=criteriaBuilder.equal(root.get("name"),name);
            Predicate addressCondition=criteriaBuilder.equal(root.get("address"),address);
            predicates.add(criteriaBuilder.and(nameCondition,addressCondition));
        }
        criteriaQuery.where(criteriaBuilder.or(predicates.toArray(new Predicate[0])));
        return entityManager.createQuery(criteriaQuery).getResultList();
    }
}
