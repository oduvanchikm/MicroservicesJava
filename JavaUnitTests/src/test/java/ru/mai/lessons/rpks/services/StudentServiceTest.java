package ru.mai.lessons.rpks.services;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import java.util.Optional;

import com.github.dockerjava.api.exception.NotFoundException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.mai.lessons.rpks.dto.mappers.StudentMapper;
import ru.mai.lessons.rpks.dto.requests.StudentCreateRequest;
import ru.mai.lessons.rpks.dto.requests.StudentUpdateRequest;
import ru.mai.lessons.rpks.dto.respones.StudentResponse;
import ru.mai.lessons.rpks.models.Student;
import ru.mai.lessons.rpks.repositories.StudentRepository;
import ru.mai.lessons.rpks.services.impl.StudentServiceImpl;

@ExtendWith(MockitoExtension.class)
class StudentServiceTest {

    @Mock
    private StudentRepository repository;

    @Mock
    private StudentMapper mapper;

    @InjectMocks
    private StudentServiceImpl service;

    @Test
    @DisplayName("Тест на поиск студента по его идентификатору")
    void givenStudentId_whenGetStudent_thenReturnStudentResponse() {
        Long studentId = 1L;
        StudentResponse expectedResponse = new StudentResponse(1L, "Domoroschenov", "М8О-411Б");
        Student expectedModel = new Student(1L, "Domoroschenov", "М8О-411Б");
        when(repository.findById(studentId)).thenReturn(Optional.of(expectedModel));
        when(mapper.modelToResponse(expectedModel)).thenReturn(expectedResponse);

        StudentResponse actualResponse = service.getStudent(studentId);

        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    @DisplayName("Successful student saving")
    void givenValidRequest_whenSaveStudent_thenReturnStudentResponse() {
        StudentCreateRequest request = new StudentCreateRequest("Ivanov", "М8О-313Б");
        Student student = new Student(1L, "Ivanov", "М8О-313Б");
        StudentResponse expectedResponse = new StudentResponse(1L, "Ivanov", "М8О-313Б");

        when(mapper.requestToModel(request)).thenReturn(student);
        when(repository.saveAndFlush(student)).thenReturn(student);
        when(mapper.modelToResponse(student)).thenReturn(expectedResponse);

        StudentResponse actualResponse = service.saveStudent(request);

        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    @DisplayName("Successful student retrieval by ID")
    void givenExistingStudentId_whenGetStudent_thenReturnStudentResponse() {
        Long studentId = 1L;
        Student student = new Student(studentId, "Petrov", "М8О-313Б");
        StudentResponse expectedResponse = new StudentResponse(studentId, "Petrov", "М8О-313Б");

        when(repository.findById(studentId)).thenReturn(Optional.of(student));
        when(mapper.modelToResponse(student)).thenReturn(expectedResponse);

        StudentResponse actualResponse = service.getStudent(studentId);

        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    @DisplayName("Attempt to get non-existent student")
    void givenNonExistingStudentId_whenGetStudent_thenThrowNotFoundException() {
        Long studentId = 999L;
        when(repository.findById(studentId)).thenReturn(Optional.empty());
        assertThrows(org.webjars.NotFoundException.class, () -> service.getStudent(studentId));
    }

    @Test
    @DisplayName("Successful student data update")
    void givenValidRequest_whenUpdateStudent_thenReturnUpdatedStudentResponse() {
        Long studentId = 1L;
        StudentUpdateRequest request = new StudentUpdateRequest(studentId, "Sidorov", "М8О-313Б");
        Student student = new Student(studentId, "Sidorov", "М8О-313Б");
        StudentResponse expectedResponse = new StudentResponse(studentId, "Sidorov", "М8О-313Б");

        when(mapper.requestToModel(request)).thenReturn(student);
        when(repository.saveAndFlush(student)).thenReturn(student);
        when(mapper.modelToResponse(student)).thenReturn(expectedResponse);

        StudentResponse actualResponse = service.updateStudent(request);

        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    @DisplayName("Successful student deletion")
    void givenExistingStudentId_whenDeleteStudent_thenReturnDeletedStudentResponse() {
        Long studentId = 1L;
        Student student = new Student(studentId, "Smirnov", "М8О-313Б");
        StudentResponse expectedResponse = new StudentResponse(studentId, "Smirnov", "М8О-313Б");

        when(repository.findById(studentId)).thenReturn(Optional.of(student));
        when(mapper.modelToResponse(student)).thenReturn(expectedResponse);

        StudentResponse actualResponse = service.deleteStudent(studentId);

        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    @DisplayName("Attempt to delete non-existent student")
    void givenNonExistingStudentId_whenDeleteStudent_thenThrowNotFoundException() {
        Long studentId = 999L;
        when(repository.findById(studentId)).thenReturn(Optional.empty());
        assertThrows(org.webjars.NotFoundException.class, () -> service.deleteStudent(studentId));
    }
}
