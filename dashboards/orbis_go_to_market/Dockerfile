FROM python:3.8-slim-buster

WORKDIR /app
COPY . /app
RUN pip install -r /app/requirements.txt
EXPOSE 8501

ENTRYPOINT ["streamlit", "run"]
CMD ["app.py"]