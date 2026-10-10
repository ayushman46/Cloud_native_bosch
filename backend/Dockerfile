FROM python:3.12-slim
WORKDIR /app
COPY backend/requirements.lock .
RUN pip install --no-cache-dir -r requirements.lock
COPY backend/chaosai ./chaosai
COPY hacker_agent_observer_contract ./hacker_agent_observer_contract
RUN useradd --uid 10001 --create-home chaosai && mkdir -p /data && chown chaosai:chaosai /data
USER chaosai
EXPOSE 8000
CMD ["uvicorn", "chaosai.api:app", "--host", "0.0.0.0", "--port", "8000"]
