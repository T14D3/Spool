package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.OrderedChild;
import de.t14d3.spool.test.entities.OrderedCourse;
import de.t14d3.spool.test.entities.OrderedParent;
import de.t14d3.spool.test.entities.OrderedStudent;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * T2.4: {@code @OrderBy} is applied to OneToMany and ManyToMany load queries,
 * for both single collection access and the batched explicit fetch.
 */
public class OrderByCollectionTest {

    @Test
    void oneToManyIsOrderedOnLazyAndBatchLoads() {
        String url = "jdbc:h2:mem:order_by_" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
        EntityManager em = EntityManager.create(url);
        Long parentId;
        try {
            em.registerEntities(OrderedParent.class, OrderedChild.class);
            em.updateSchema();

            OrderedParent parent = new OrderedParent("p");
            for (String name : List.of("alpha", "charlie", "bravo")) {
                OrderedChild child = new OrderedChild(name);
                child.setParent(parent);
                parent.getChildren().add(child);
            }
            em.persist(parent);
            em.flush();
            parentId = parent.getId();
        } finally {
            em.close();
        }

        EntityManager reader = EntityManager.create(url);
        try {
            reader.registerEntities(OrderedParent.class, OrderedChild.class);
            OrderedParent loaded = reader.find(OrderedParent.class, parentId);
            assertEquals(List.of("charlie", "bravo", "alpha"), names(loaded.getChildren()));

            OrderedParent reloaded = reader.find(OrderedParent.class, parentId);
            reader.initialize(reloaded);
            assertEquals(List.of("charlie", "bravo", "alpha"), names(reloaded.getChildren()));

            List<OrderedParent> all = reader.findAll(OrderedParent.class);
            reader.initializeAll(all);
            assertEquals(List.of("charlie", "bravo", "alpha"), names(all.get(0).getChildren()));
        } finally {
            reader.close();
        }
    }

    @Test
    void manyToManyIsOrderedOnLazyAndBatchLoads() {
        String url = "jdbc:h2:mem:order_by_m2m_" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
        EntityManager em = EntityManager.create(url);
        Long studentId;
        try {
            em.registerEntities(OrderedStudent.class, OrderedCourse.class);
            em.updateSchema();

            OrderedStudent student = new OrderedStudent("s");
            for (String title : List.of("Alpha", "Charlie", "Bravo")) {
                OrderedCourse course = new OrderedCourse(title);
                student.getCourses().add(course);
                em.persist(course);
            }
            em.persist(student);
            em.flush();
            studentId = student.getId();
        } finally {
            em.close();
        }

        EntityManager reader = EntityManager.create(url);
        try {
            reader.registerEntities(OrderedStudent.class, OrderedCourse.class);
            OrderedStudent loaded = reader.find(OrderedStudent.class, studentId);
            assertEquals(List.of("Charlie", "Bravo", "Alpha"),
                    loaded.getCourses().stream().map(OrderedCourse::getTitle).collect(Collectors.toList()));

            List<OrderedStudent> all = reader.findAll(OrderedStudent.class);
            reader.initializeAll(all);
            assertEquals(List.of("Charlie", "Bravo", "Alpha"),
                    all.get(0).getCourses().stream().map(OrderedCourse::getTitle).collect(Collectors.toList()));
        } finally {
            reader.close();
        }
    }

    private static List<String> names(List<OrderedChild> children) {
        return children.stream().map(OrderedChild::getName).collect(Collectors.toList());
    }
}
