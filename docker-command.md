# init docker
## create docker network
docker network create default_network
..

# airflow
..
docker-compose up -d --build
..

# spark
..
docker-compose up -d
..

# kafka
..
docker-compose up -d
..

# postgresql
..
docker run --name postgres-ds9 --network=default_network -e POSTGRES_PASSWORD=root -p 5432:5432 -d postgres
..

# mysql
..
docker run --name mysql-ds9 --network=default_network --hostname mysql -e MYSQL_ROOT_PASSWORD=root -p 3306:3306 -d mysql
..