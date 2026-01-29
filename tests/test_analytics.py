
import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F
from src.analytics.kpi_calculator import SparkKPICalculator
from src.analytics.aggregations import SparkMovieAggregations

class TestKPICalculator:
    """Test SparkKPICalculator class."""
    
    @pytest.fixture
    def kpi_data(self, spark):
        """Create sample data for KPI calculations."""
        data = [
            {"title": "High Rev", "revenue_musd": 100.0, "budget_musd": 50.0, "vote_count": 100, "vote_average": 8.0, "popularity": 10.0, "release_date": "2020-01-01"},
            {"title": "Low Rev", "revenue_musd": 10.0, "budget_musd": 5.0, "vote_count": 5, "vote_average": 9.0, "popularity": 1.0, "release_date": "2021-01-01"},
            {"title": "Flop", "revenue_musd": 1.0, "budget_musd": 20.0, "vote_count": 50, "vote_average": 2.0, "popularity": 5.0, "release_date": "2022-01-01"},
        ]
        return spark.createDataFrame(data)

    def test_rank_movies_revenue(self, spark, kpi_data):
        calc = SparkKPICalculator(spark)
        # Test getting top revenue
        top_rev = calc.rank_movies(kpi_data, "revenue_musd", ascending=False, top_n=2)
        rows = top_rev.collect()
        
        assert len(rows) == 2
        assert rows[0]['rank'] == 1
        assert rows[0]['title'] == "High Rev"

    def test_get_top_by_roi(self, spark, kpi_data):
        calc = SparkKPICalculator(spark)
        # High Rev: (100-50)/50 = 100% ROI. Budget >= 10.
        # Low Rev: Budget 5 < 10. Should be filtered out.
        # Flop: (1-20)/20 = -95% ROI. Budget 20 >= 10.
        
        result = calc.get_top_by_roi(kpi_data, top_n=10)
        rows = result.collect()
        
        # Should only have High Rev and Flop. Low Rev filtered by budget.
        assert len(rows) == 2
        assert rows[0]['title'] == "High Rev"
        assert rows[1]['title'] == "Flop"

    def test_get_top_rated_filter(self, spark, kpi_data):
        calc = SparkKPICalculator(spark)
        # "Low Rev" has high rating (9.0) but low votes (5 < 10). Should be filtered.
        
        result = calc.get_top_rated(kpi_data, top_n=10)
        rows = result.collect()
        
        titles = [r['title'] for r in rows]
        assert "Low Rev" not in titles
        assert "High Rev" in titles

class TestAggregations:
    """Test SparkMovieAggregations class."""
    
    @pytest.fixture
    def agg_data(self, spark):
        """Create sample data for aggregations."""
        data = [
            {"title": "F1", "collection_name": "Franchise A", "director": "Director X", "revenue_musd": 100.0, "budget_musd": 50.0, "vote_average": 7.0, "popularity": 10.0, "vote_count": 100},
            {"title": "F2", "collection_name": "Franchise A", "director": "Director X", "revenue_musd": 150.0, "budget_musd": 60.0, "vote_average": 8.0, "popularity": 12.0, "vote_count": 120},
            {"title": "S1", "collection_name": None, "director": "Director Y", "revenue_musd": 40.0, "budget_musd": 20.0, "vote_average": 6.0, "popularity": 5.0, "vote_count": 50},
        ]
        return spark.createDataFrame(data)

    def test_compare_franchise_vs_standalone(self, spark, agg_data):
        agg = SparkMovieAggregations(spark)
        result = agg.compare_franchise_vs_standalone(agg_data)
        rows = result.collect()
        
        # Should have 2 rows: Franchise and Standalone
        assert len(rows) == 2
        
        franchise_row = next(r for r in rows if r['is_franchise'] == 'Franchise')
        standalone_row = next(r for r in rows if r['is_franchise'] == 'Standalone')
        
        assert franchise_row['movie_count'] == 2
        # Mean rev: (100+150)/2 = 125
        assert franchise_row['mean_revenue_musd'] == 125.0
        
        assert standalone_row['movie_count'] == 1
        assert standalone_row['mean_revenue_musd'] == 40.0

    def test_get_top_franchises(self, spark, agg_data):
        agg = SparkMovieAggregations(spark)
        result = agg.get_top_franchises(agg_data, top_n=5)
        rows = result.collect()
        
        assert len(rows) == 1
        assert rows[0]['collection_name'] == "Franchise A"
        assert rows[0]['total_revenue_musd'] == 250.0 # 100 + 150

    def test_get_top_directors(self, spark, agg_data):
        agg = SparkMovieAggregations(spark)
        result = agg.get_top_directors(agg_data, top_n=5)
        rows = result.collect()
        
        # Director X has 2 movies, Y has 1
        director_x = next(r for r in rows if r['director'] == "Director X")
        assert director_x['movie_count'] == 2
        assert director_x['total_revenue_musd'] == 250.0
