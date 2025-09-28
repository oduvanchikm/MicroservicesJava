package ru.mai.lessons.rpks.controllers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import lombok.SneakyThrows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import ru.mai.lessons.rpks.controllers.impl.StudentControllerImpl;
import ru.mai.lessons.rpks.dto.requests.StudentCreateRequest;
import ru.mai.lessons.rpks.dto.respones.StudentResponse;
import ru.mai.lessons.rpks.services.StudentService;
import ru.mai.lessons.rpks.utils.JsonUtils;

@AutoConfigureMockMvc
@WebMvcTest(StudentControllerImpl.class)
@TestPropertySource(properties = "server.port=8080")
class StudentControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private StudentService service;

    @Test
    @SneakyThrows
    @DisplayName("Тест на поиск студента по его идентификатору")
    void givenStudentId_whenGetStudent_thenReturnStudentResponse_Positive() {
        StudentResponse expectedResponse = new StudentResponse();
        when(service.getStudent(1L)).thenReturn(expectedResponse);

        mockMvc
                .perform(
                        get("/student/get")
                                .param("id", "1")
                )
                .andExpect(status().isOk())
                .andExpect(content().string(JsonUtils.toJson(expectedResponse)));
    }

    @Test
    @SneakyThrows
    @DisplayName("Unsuccessful search of student by ID")
    void givenStudentId_whenGetStudent_thenReturnStudentResponse_Negative() {
        when(service.getStudent(99898L)).thenThrow(new RuntimeException("Student not found"));

        mockMvc.perform(
                        get("/student/get")
                                .param("id", "99898")
                )
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.error").value("Student not found"));
    }

    @Test
    @SneakyThrows
    @DisplayName("Saving a student with correct data")
    void givenStudentId_whenSaveStudent_thenReturnStudentResponse_Positive() {
        StudentCreateRequest request = new StudentCreateRequest("John", "113Б-24");
        StudentResponse expectedResponse = new StudentResponse(1L, "John", "113Б-24");

        when(service.saveStudent(any())).thenReturn(expectedResponse);

        mockMvc
                .perform(
                        post("/student/save")
                                .contentType("application/json")
                                .content(JsonUtils.toJson(request))
                )
                .andExpect(status().isOk())
                .andExpect(content().json(JsonUtils.toJson(request)));
    }

    @Test
    @SneakyThrows
    @DisplayName("Saving a student with incorrect data")
    void givenStudentId_whenSaveStudent_thenReturnStudentResponse_Negative() {
        StudentCreateRequest invalid = new StudentCreateRequest("", "");

        mockMvc
                .perform(
                        post("/student/save")
                                .contentType("application/json")
                                .content(JsonUtils.toJson(invalid))
                )
                .andExpect(status().isUnprocessableEntity());
    }

    @Test
    @SneakyThrows
    @DisplayName("Error saving student")
    void givenValidStudent_whenSaveStudent_thenReturnErrorResponse_Negative() {
        StudentCreateRequest validRequest = new StudentCreateRequest("John", "М8О-113Б");

        when(service.saveStudent(any())).thenThrow(new RuntimeException("Error saving student"));

        mockMvc
                .perform(
                        post("/student/save")
                                .contentType("application/json")
                                .content(JsonUtils.toJson(validRequest))
                )
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.error").value("Error saving student"));
    }

    @Test
    @SneakyThrows
    @DisplayName("Deleting a student with correct data")
    void givenStudentId_whenDeleteStudent_thenReturnStudentResponse_Positive() {
        StudentResponse response = new StudentResponse();

        when(service.deleteStudent(52L)).thenReturn(response);

        mockMvc
                .perform(
                        delete("/student/delete")
                                .param("id", "52")
                )
                .andExpect(status().isOk())
                .andExpect(content().json(JsonUtils.toJson(response)));
    }

    @Test
    @SneakyThrows
    @DisplayName("Deleting a student with incorrect data")
    void givenStudent_whenDeleteStudent_thenReturnErrorResponse_Negative() {
        when(service.deleteStudent(99898L)).thenThrow(new RuntimeException("Student not found"));

        mockMvc.perform(
                        delete("/student/delete")
                                .param("id", "99898")
                )
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.error").value("Student not found"));
    }
}
