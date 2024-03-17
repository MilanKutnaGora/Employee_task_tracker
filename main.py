from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import psycopg2

app = FastAPI()

# Модель задачи
class Task(BaseModel):
    name: str
    parent_id: int = None
    executor: str
    deadline: str
    status: str

# Модель сотрудника
class Employee(BaseModel):
    name: str
    position: str

# Функция для получения соединения с БД
def get_db_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="tasks_db",
        user="postgres",
        password="milkut56466"
    )
    return conn

# Функция для получения списка сотрудников и их задач, отсортированного по количеству активных задач
@app.get("/employees/busy")
def get_busy_employees():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT e.name, e.position, COUNT(t.id) as task_count FROM employees e LEFT JOIN tasks t ON e.id = t.executor_id GROUP BY e.id ORDER BY task_count DESC")
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return {"employees": [{"name": row[0], "position": row[1], "tasks_count": row[2]} for row in result]}

# Функция для получения списка важных задач
@app.get("/tasks/important")
def get_important_tasks():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT t.id, t.name, t.deadline, e.name FROM tasks t LEFT JOIN employees e ON e.id = t.executor_id WHERE t.parent_id IS NULL AND e.id IS NULL ORDER BY t.deadline ASC")
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return {"tasks": [{"id": row[0], "name": row[1], "deadline": row[2], "executor": row[3]} for row in result]}

# Функция для добавления новой задачи
@app.post("/tasks/add")
def add_task(task: Task):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO tasks (name, parent_id, executor_id, deadline, status) VALUES (%s, %s, %s, %s, %s)", (task.name, task.parent_id, task.executor, task.deadline, task.status))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Task added successfully"}

# Функция для добавления нового сотрудника
@app.post("/employees/add")
def add_employee(employee: Employee):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO employees (name, position) VALUES (%s, %s)", (employee.name, employee.position))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Employee added successfully"}

# Функция для обновления существующей задачи
@app.put("/tasks/{task_id}")
def update_task(task_id: int, task: Task):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE tasks SET name=%s, parent_id=%s, executor_id=%s, deadline=%s, status=%s WHERE id=%s", (task.name, task.parent_id, task.executor, task.deadline, task.status, task_id))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Task updated successfully"}

# Функция для обновления существующего сотрудника
@app.put("/employees/{employee_id}")
def update_employee(employee_id: int, employee: Employee):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE employees SET name=%s, position=%s WHERE id=%s", (employee.name, employee.position, employee_id))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Employee updated successfully"}

# Функция для удаления задачи
@app.delete("/tasks/{task_id}")
def delete_task(task_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM tasks WHERE id=%s", (task_id,))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Task deleted successfully"}

# Функция для удаления сотрудника
@app.delete("/employees/{employee_id}")
def delete_employee(employee_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM employees WHERE id=%s", (employee_id,))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Employee deleted successfully"}
