FROM postgres
ENV POSTGRES_USER chessDB
ENV POSTGRES_PASSWORD chessDB
ADD schema.sql /docker-entrypoint-initdb.d
EXPOSE 5432