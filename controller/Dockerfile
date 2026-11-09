FROM python:3.12-slim
WORKDIR /app
COPY controller/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY controller/chaos_controller ./chaos_controller
EXPOSE 9000
CMD ["uvicorn", "chaos_controller.main:app", "--host", "0.0.0.0", "--port", "9000"]
