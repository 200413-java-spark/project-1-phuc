FROM postgres
ENV POSTGRES_USER chessdb
ENV POSTGRES_PASSWORD chessdb
ADD schema.sql /docker-entrypoint-initdb.d
EXPOSE 5432