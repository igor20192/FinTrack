# FinTrack

FinTrack is an asynchronous Python web application built with FastAPI for managing financial data, including credits, payments, and plans. It provides an API for analyzing yearly performance, user credits, and plan execution, with Redis caching to enhance performance.

## Features
- **GET /year_performance**: Retrieves issuance and collection performance by month for a specified year.
- **GET /user_credits/{user_id}**: Fetches credit details for a user, including payment stats and overdue information.
- **GET /plans_performance**: Returns plan performance up to a given date.
- **POST /plans_insert**: Imports new plans from a CSV file into the database.
- **Caching**: Utilizes Redis to speed up repeated requests.
- **Database**: MySQL for storing user, credit, payment, and plan data.

## Technologies
- **Python**: 3.12
- **FastAPI**: Asynchronous web framework
- **SQLAlchemy**: ORM for MySQL interaction
- **Redis**: In-memory caching
- **Docker**: Containerization for MySQL and Redis
- **Pydantic**: Data validation and response modeling

## Project Structure

```bash
FinTrack/
├── app/
│   ├── init.py
│   ├── main.py          # FastAPI entry point
│   ├── crud.py          # Data processing logic
│   ├── cache.py         # Redis caching utilities
│   ├── database.py      # Database connection setup
│   ├── models.py        # SQLAlchemy models
│   └── load_data.py     # Script for initial data loading
├── docker-compose.yml   # Docker configuration for MySQL and Redis
├── .env.db              # MySQL environment variables
├── .env                 # Environment variables
├── requirements.txt     # Python dependencies
└── README.md            # This file

```


## Requirements
- **Python**: 3.12 (recommended to use `pyenv` for version management)
- **Docker**: For running MySQL and Redis
- **Docker Compose**: For managing containers

## Installation and Setup

### 1. Clone the Repository
```bash
git clone git@github.com:igor20192/FinTrack.git
cd FinTrack
```

### 2. Set Up Virtual Environment

Use `pyenv` and `virtualenv` to create an isolated environment:

```bash
pyenv install 3.12.7
pyenv virtualenv 3.12.7 FinTrack
pyenv local FinTrack
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Create `.env.db` and `.env` files in the project root.

`.env.db`
```bash
MYSQL_ROOT_PASSWORD=root
MYSQL_DATABASE=track_db
MYSQL_USER=user
MYSQL_PASSWORD=password
```
`.env`
```bash
MYSQL_ROOT_PASSWORD=rootpassword
MYSQL_DATABASE=track_db
MYSQL_USER=track
MYSQL_PASSWORD= password
```

### 5. Launch Docker Containers

Start MySQL and Redis using Docker Compose:

```bash
docker-compose up -d
```

Verify container status:

```bash
docker-compose ps
```

### 6. Load Initial Data

The `load_data.py` script populates the database from CSV files. Prepare the following files in the project root and run the script:

#### Example CSV Files

- `users.csv`:
```bash
   id	login	registration_date
   1	devotedclinic	01.01.2020
   2	okayhurried	01.01.2020
   3	blinkhour	01.01.2020
   4	coaminggasbag	02.01.2020
   5	hatefullanky	02.01.2020
   6	reindeerserene	02.01.2020
   7	jibboombento	02.01.2020
   8	paprikarank	02.01.2020
   9	emissionbalk	03.01.2020
   10	retinaspoonbill	03.01.2020
```

- `dictionary.csv`:
```bash
id	name
1	тіло
2	відсотки
3	видача
4	збір
```

- `credits.csv`:
```bash
id	user_id	issuance_date	return_date	actual_return_date	body	percent
1	31	11.01.2020	25.01.2020	23.04.2021	4500	32535
2	19	12.01.2020	26.01.2020	30.08.2020	4500	16537.5
3	22	20.01.2020	03.02.2020	23.11.2020	1500	7245
4	97	28.01.2020	11.02.2020	08.06.2020	5000	10950
5	37	13.02.2020	27.02.2020	10.03.2021	4000	24300
6	121	17.02.2020	02.03.2020	11.08.2021	1000	8325
7	151	23.02.2020	08.03.2020	23.08.2021	1000	8415
8	96	25.02.2020	10.03.2020	16.04.2021	2500	16125
9	197	27.02.2020	12.03.2020	11.09.2021	4500	38880
10	157	01.03.2020	15.03.2020	25.03.2020	1500	855
```

- `plans.csv`:
```bash
id	period	sum	category_id
1	01.01.2020	21000	3
2	01.01.2020	5000	4
3	01.02.2020	11000	3
4	01.02.2020	16000	4
5	01.03.2020	52000	3
6	01.03.2020	53000	4
7	01.04.2020	71000	3
8	01.04.2020	63000	4
9	01.05.2020	79000	3
10	01.05.2020	120000	4
```

- `payments.csv`:
```bash
id	credit_id	payment_date	type_id	sum
1	2	14.01.2020	2	1837.50
2	3	23.01.2020	1	136.36
3	1	26.01.2020	2	2033.44
4	4	29.01.2020	1	555.56
5	4	02.02.2020	1	555.56
6	4	02.02.2020	1	555.56
7	2	09.02.2020	2	1837.50
8	4	09.02.2020	2	912.50
9	4	11.02.2020	2	912.50
10	3	12.02.2020	2	226.41
```

#### Run the script:

```bash
python app/load_data.py
```

### 7. Start the Application

Launch the FastAPI server:

```bash
python app/main.py
```

The server will be available at `http://localhost:8000`.

## API Usage

### Endpoints

1. GET /year_performance

    - Parameters: `year` (int) — Year to analyze.
    - Example: `GET http://localhost:8000/year_performance?year=2021`
    - Response: List of monthly performance objects.

2. GET /user_credits/{user_id}

    - Parameters: `user_id` (int) — User ID.

    - Example: `GET http://localhost:8000/user_credits/10`

    - Response: List of user credits with payment details.


3. GET /plans_performance

    - Parameters: `check_date` (date, `YYYY-MM-DD` format) — Date to evaluate plan performance.
    - Example: `GET http://localhost:8000/plans_performance?check_date=2021-12-31`
    - Response: List of plan performance objects.

4. POST /plans_insert

    - Body: CSV file with columns `month, category_name, sum`.
    - Example:
        - In Postman, use `form-data`, key `file`, upload `plans.xlsx`.
        - File content:
        ```bash
        month,category_name,sum
        2021-01-01,Issuance,10000
        ```
    - Response: `{"message": "Plans inserted successfully"}`


## Sample Response for `/year_performance`

```json
[
  {
    "month_year": "2021-01",
    "issuance_count": 5,
    "plan_issuance_sum": 10000,
    "actual_issuance_sum": 9500,
    "issuance_performance_percent": 95.0,
    "payment_count": 3,
    "plan_collection_sum": 5000,
    "actual_collection_sum": 4800,
    "collection_performance_percent": 96.0,
    "issuance_percent_of_year": 10.0,
    "collection_percent_of_year": 8.0
  }
]
```

## Performance

- Caching: Repeated requests are processed in ~5 ms via Redis.

- Initial Request: ~0.5 seconds (varies with data volume).

- Cache Invalidation: Automatically triggered on new plan insertion via `/plans_insert`.


## Additional Instructions

- Verify Database Connection:

Connect to MySQL to inspect the database:
```bash
    docker-compose exec db mysql -uuser -ppassword track_db
```

Run a query:

```sql
SHOW TABLES;
```

- Inspect Redis Cache:

Connect to Redis to check cached data:

```bash
docker-compose exec redis redis-cli -a redis_password
> KEYS *
```

- Reset and Reload Data:
To reset the database and reload data:

```bash
docker-compose down -v
docker-compose up -d
python app/load_data.py
```

## Stopping the Project

Stop Docker containers:

```bash
docker-compose down
```

For a full cleanup (including data):

```bash
docker-compose down -v
```

## Development and Debugging

- Logs are set to `INFO` level by default. For detailed logs, adjust in `crud.py`:
```python
logging.basicConfig(level=logging.DEBUG)
```

- Use Postman for testing endpoints.

## Potential Improvements

- Add authentication to secure the API.

- Implement pagination for large datasets.

- Optimize SQL queries with additional indexes.






























































