# TMDB Movie Analytics with Apache Spark

A comprehensive movie analytics platform leveraging Apache Spark for large-scale data processing and analysis of TMDB (The Movie Database) data. This project demonstrates advanced data engineering and analytics techniques using PySpark, with a focus on scalability, performance, and insightful visualizations.

## Project Overview

This project performs in-depth analysis of movie data from TMDB, processing thousands of movies to extract meaningful insights about revenue, popularity, ratings, genres, franchises, and more. Built entirely with Apache Spark for distributed computing, it showcases best practices in big data processing and analytics.

### Key Highlights

- **Distributed Processing**: Leverages PySpark for handling large-scale movie datasets
- **13 KPI Metrics**: Comprehensive performance indicators for movie analysis
- **Advanced Analytics**: Franchise comparisons, director performance, genre analysis
- **Rich Visualizations**: Interactive charts and graphs using Matplotlib and Seaborn
- **Production-Ready**: Dockerized environment with Jupyter notebooks for exploration

## Features

### 1. Data Collection & Processing

- **TMDB API Integration**: Automated data fetching from TMDB API
- **Data Cleaning Pipeline**: Robust PySpark-based cleaning with custom UDFs
- **Schema Validation**: Type conversions and data quality checks
- **Multi-stage Processing**: Fetch → Clean → Analyze workflow

### 2. Key Performance Indicators (KPIs)

The project calculates 13 comprehensive KPIs:

| KPI Category   | Metrics                                          |
| -------------- | ------------------------------------------------ |
| **Financial**  | Top Revenue, Top Budget, Top Profit, Highest ROI |
| **Popularity** | Most Popular, Most Voted                         |
| **Quality**    | Highest Rated, Best Value (Budget/Rating)        |
| **Franchise**  | Top Franchise Revenue, Top Standalone Revenue    |
| **Director**   | Top Director Revenue, Top Director Rating        |
| **Genre**      | Top Genre by Revenue                             |

### 3. Advanced Filtering

- **Genre-based filtering**: Find movies by specific genres
- **Cast filtering**: Search movies by actor names
- **Director filtering**: Filter by director
- **Complex queries**: Multi-criteria search capabilities
- **Pre-built searches**:
  - Sci-Fi Action movies with specific actors
  - Director-specific genre combinations

### 4. Aggregation Analytics

- **Franchise vs Standalone Comparison**: Statistical analysis of franchise movies versus standalone films
- **Top Franchises**: Revenue, budget, and performance metrics
- **Top Directors**: Comprehensive director analytics with movie counts and quality metrics
- **Detailed Reports**: Individual franchise and director breakdowns

### 5. Data Visualizations

Five comprehensive visualizations:

1. **Revenue vs Budget Scatter Plot**: Correlation analysis with profit markers
2. **ROI by Genre**: Box plots showing return on investment across genres
3. **Popularity vs Rating**: Relationship between audience engagement and quality
4. **Yearly Box Office Trends**: Multi-metric time series analysis
5. **Franchise vs Standalone**: Comparative bar charts across key metrics

## Project Structure

```
tmbd_api_analysis_spark/
├── docker/                          # Docker configuration
│   ├── docker-compose.yml          # Service orchestration
│   └── Dockerfile                  # Custom Jupyter + Spark image
├── notebooks/                       # Analysis notebooks
│   └── pipeline.ipynb              # Main analysis pipeline
├── src/                            # Source code
│   ├── analytics/                  # Analytics modules
│   │   ├── kpi_calculator.py      # KPI calculations
│   │   ├── filters.py             # Advanced filtering
│   │   └── aggregations.py        # Aggregation functions
│   ├── viz/                        # Visualization modules
│   │   └── visualizations.py      # Chart generation
│   ├── cleaning/                   # Data cleaning
│   │   ├── cleaner.py             # Main cleaning pipeline
│   │   └── udfs.py                # Custom UDFs
│   └── fetching/                   # Data fetching
│       └── fetcher.py             # TMDB API client
├── tests/                          # Test suite
│   ├── test_phase2_fetch.py       # Fetch tests
│   ├── test_phase3_clean.py       # Cleaning tests
│   └── test_phase4_kpi.py         # KPI tests
└── README.md                       # This file
```

## Getting Started

### Prerequisites

- Docker Desktop
- TMDB API Key ([Get one here](https://www.themoviedb.org/settings/api))

### Installation

1. **Clone the repository**

   ```bash
   git clone <repository-url>
   cd tmbd_api_analysis_spark
   ```

2. **Set up environment variables**

   ```bash
   # Create .env file in docker/ directory
   echo "TMDB_API_KEY=your_api_key_here" > docker/.env
   ```

3. **Start the Docker environment**

   ```bash
   cd docker
   docker-compose up
   ```

4. **Access Jupyter Notebook**
   - Open browser to: `http://localhost:8888`
   - Token is displayed in the terminal output

### Quick Start

1. Open `notebooks/pipeline.ipynb` in Jupyter
2. Run cells sequentially to:
   - Initialize Spark session
   - Fetch movie data from TMDB
   - Clean and process data
   - Calculate KPIs
   - Generate visualizations

## Usage Examples

### Calculating KPIs

```python
from src.analytics import SparkKPICalculator

# Initialize calculator
kpi_calc = SparkKPICalculator()

# Get top revenue movies
top_revenue = kpi_calc.top_revenue_movies(cleaned_df, top_n=10)

# Calculate highest ROI movies (with filters)
high_roi = kpi_calc.rank_movies(
    cleaned_df,
    metric='roi',
    filter_condition=(F.col('budget_musd') > 1),
    top_n=10
)
```

### Advanced Filtering

```python
from src.analytics import SparkMovieFilters

filters = SparkMovieFilters()

# Find Action movies
action_movies = filters.filter_by_genre(cleaned_df, 'Action')

# Complex search: Sci-Fi Action with Bruce Willis
results = filters.search_scifi_action_bruce_willis(cleaned_df)
```

### Aggregations

```python
from src.analytics import SparkMovieAggregations

agg = SparkMovieAggregations()

# Compare franchises vs standalone movies
comparison = agg.compare_franchise_vs_standalone(cleaned_df)

# Get top franchises
top_franchises = agg.get_top_franchises(cleaned_df, top_n=10)
```

### Visualizations

```python
from src.viz import MovieVisualizer

viz = MovieVisualizer(spark=spark)

# Create revenue vs budget plot
fig1 = viz.plot_revenue_vs_budget(cleaned_df)

# ROI by genre
fig2 = viz.plot_roi_by_genre(cleaned_df, top_n_genres=10)
```

## Technology Stack

| Component                 | Technology                         |
| ------------------------- | ---------------------------------- |
| **Distributed Computing** | Apache Spark 3.5+                  |
| **Data Processing**       | PySpark, Pandas                    |
| **API Client**            | requests (TMDB API)                |
| **Visualization**         | Matplotlib, Seaborn                |
| **Environment**           | Docker, Jupyter Lab                |
| **Base Image**            | quay.io/jupyter/all-spark-notebook |

## Key Insights & Analytics

### Performance Metrics

- **Processing Speed**: Handles 10,000+ movies efficiently using Spark's distributed computing
- **Memory Optimization**: Lazy evaluation and Spark optimizations for large datasets
- **Code Reusability**: Modular design with reusable `rank_movies` function at core

### Analysis Capabilities

1. **Financial Analysis**: Revenue, budget, profit, and ROI calculations
2. **Popularity Metrics**: Vote counts and popularity scores
3. **Quality Assessment**: Rating analysis with vote thresholds
4. **Franchise Performance**: Comparing franchise vs standalone success
5. **Director Analytics**: Director-level performance aggregations
6. **Genre Trends**: Genre-based revenue and ROI analysis

## Testing

Run the test suite:

```bash
# Inside the container
pytest tests/
```

Test coverage includes:

- Data fetching validation
- Cleaning pipeline tests
- KPI calculation accuracy
- Filter functionality
- Aggregation correctness

## 📝 Data Pipeline

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────────┐
│   TMDB API  │────▶│   Cleaning   │────▶│  Analytics  │────▶│Visualization │
│   Fetcher   │     │   Pipeline   │     │ (KPIs, Agg) │     │   Charts     │
└─────────────┘     └──────────────┘     └─────────────┘     └──────────────┘
      │                    │                     │                    │
   Raw JSON           PySpark UDFs         Spark SQL            Matplotlib
   requests           Type casting         Window funcs         Seaborn
```

## 🎓 Learning Outcomes

This project demonstrates:

- **PySpark Mastery**: DataFrames, SQL functions, Window operations, UDFs
- **Data Engineering**: ETL pipelines, data cleaning, schema management
- **Analytics**: Statistical analysis, aggregations, KPI calculations
- **Visualization**: Hybrid Spark + Pandas approach for plotting
- **Best Practices**: Modular code, testing, documentation, Docker deployment

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs and feature requests.

## License

This project is for educational purposes. Movie data is provided by TMDB and subject to their terms of service.

## Acknowledgments

- **TMDB** for providing the extensive movie database API
- **Apache Spark** community for the powerful distributed computing framework
- **Jupyter Project** for the excellent notebook environment

---

**Built using Apache Spark**

_For questions or feedback, please open an issue on GitHub._
