package ru.mai.lessons.rpks.repositories;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.test.context.ActiveProfiles;
import ru.mai.lessons.rpks.models.Student;

@DataJpaTest
@ActiveProfiles("test")
@AutoConfigureTestDatabase(replace = Replace.NONE)
class StudentRepositoryTest {

    @Autowired
    private StudentRepository repository;

    @BeforeEach
    public void setUp() {
        repository.deleteAll();
    }

    @Test
    @DisplayName("Тест на поиск студента по его идентификатору")
    void givenStudent_whenFindById_thenReturnStudent() {
        Student studentToSave = new Student(null, "Domoroschenov", "М8О-411Б");
        Student savedStudent = repository.save(studentToSave);

        Student studentById = repository.findById(savedStudent.getId())
                .orElse(null);

        assertEquals(studentToSave.getFullName(), studentById.getFullName());
        assertEquals(studentToSave.getGroupName(), studentById.getGroupName());
    }

    @Test
    @DisplayName("Save student with correct data")
    void givenStudent_whenSavedById_thenReturnStudent_Positive() {
        Student studentToSave = new Student(null, "Domoroschenov", "М8О-313Б");
        Student savedStudent = repository.save(studentToSave);

        assertNotNull(savedStudent.getId());
        assertEquals(studentToSave.getFullName(), savedStudent.getFullName());
        assertEquals(studentToSave.getGroupName(), savedStudent.getGroupName());
    }

    @Test
    @DisplayName("Update student with correct data")
    void givenStudent_whenUpdatedById_thenReturnStudent_Positive() {
        Student studentToSave = new Student(null, "Person", "М8О-313Б");
        Student savedStudent = repository.save(studentToSave);

        savedStudent.setFullName("newPerson");
        savedStudent.setGroupName("newМ8О-313Б");

        Student updatedStudent = repository.save(savedStudent);

        assertEquals("newPerson", updatedStudent.getFullName());
        assertEquals("newМ8О-313Б", updatedStudent.getGroupName());
    }

    @Test
    @DisplayName("Delete student by ID")
    void givenStudent_whenDeletedById_thenReturnStudent_Positive() {
        Student student = new Student(null, "Person", "М8О-313Б");
        Student savedStudent = repository.save(student);

        Assertions.assertTrue(repository.findById(savedStudent.getId()).isPresent());
        repository.deleteById(savedStudent.getId());
        Assertions.assertFalse(repository.findById(savedStudent.getId()).isPresent());
    }

    @Test
    @DisplayName("Search for non-existent student")
    void givenInvalidId_whenFindById_thenReturnEmpty() {
        var foundStudent = repository.findById(543L);
        Assertions.assertFalse(foundStudent.isPresent());
    }

    @Test
    @DisplayName("Save student with incorrect data")
    void givenInvalidStudent_whenSave_thenThrowException() {
        Student invalidStudent = new Student(null, null, null);
        assertThrows(DataIntegrityViolationException.class, () -> repository.save(invalidStudent));
    }

    @Test
    @DisplayName("Update non-existent student")
    void givenStudent_whenUpdatedById_thenThrowException() {
        Student invalidStudent = new Student(6578252L, "Name", "Group");
        assertThrows(ObjectOptimisticLockingFailureException.class, () -> {
            repository.save(invalidStudent);
        });
    }

    @Test
    @DisplayName("Delete non-existent student")
    void givenInvalidId_whenDeleteById_thenThrowException() {
        Long invalidId = 543L;
        long initialCount = repository.count();

        Assertions.assertFalse(repository.findById(invalidId).isPresent());

        repository.deleteById(invalidId);
        long countAfterDelete = repository.count();

        assertEquals(initialCount, countAfterDelete, "The number of records should not change.");
    }
}
