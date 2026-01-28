"""
User-Defined Functions (UDFs) for Spark data cleaning.

Contains UDFs for extracting and transforming nested JSON structures.
"""

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, ArrayType, StructType, StructField


@udf(StringType())
def extract_collection_name(collection_struct):
    """
    Extract collection name from belongs_to_collection struct.
    
    Args:
        collection_struct: Struct containing collection information
        
    Returns:
        str: Collection name or None
    """
    if collection_struct is not None:
        return collection_struct.name
    return None


@udf(StringType())
def extract_names_from_array(array_data, separator='|'):
    """
    Extract pipe-separated names from array of structs.
    
    Used for: genres, production_companies, production_countries, spoken_languages
    
    Args:
        array_data: Array of structs with 'name' field
        separator: Separator string (default: '|')
        
    Returns:
        str: Pipe-separated names or None
    """
    if array_data is not None and len(array_data) > 0:
        names = [item.name for item in array_data if item.name is not None]
        return separator.join(names) if names else None
    return None


@udf(StringType())
def extract_keywords_from_struct(keywords_struct, separator='|'):
    """
    Extract keywords from the keywords struct.
    
    The TMDB API returns keywords as: {"keywords": [{...}, {...}]}
    
    Args:
        keywords_struct: Struct containing keywords array
        separator: Separator string (default: '|')
        
    Returns:
        str: Pipe-separated keyword names or None
    """
    if keywords_struct is not None and keywords_struct.keywords is not None:
        names = [kw.name for kw in keywords_struct.keywords if kw.name is not None]
        return separator.join(names) if names else None
    return None


@udf(StringType())
def extract_top_cast(credits_struct, top_n=5, separator='|'):
    """
    Extract top N cast members from credits.
    
    Args:
        credits_struct: Credits struct containing cast array
        top_n: Number of top cast members to extract (default: 5)
        separator: Separator string (default: '|')
        
    Returns:
        str: Pipe-separated cast names or None
    """
    if credits_struct is not None and credits_struct.cast is not None:
        # Get cast names, limit to top_n
        cast_list = credits_struct.cast[:top_n]
        names = [member.name for member in cast_list if member.name is not None]
        return separator.join(names) if names else None
    return None


@udf(IntegerType())
def get_cast_size(credits_struct):
    """
    Get total number of cast members.
    
    Args:
        credits_struct: Credits struct containing cast array
        
    Returns:
        int: Number of cast members or 0
    """
    if credits_struct is not None and credits_struct.cast is not None:
        return len(credits_struct.cast)
    return 0


@udf(StringType())
def extract_director(credits_struct):
    """
    Extract director name from credits.
    
    Args:
        credits_struct: Credits struct containing crew array
        
    Returns:
        str: Director name or None
    """
    if credits_struct is not None and credits_struct.crew is not None:
        # Find first crew member with job == 'Director'
        for crew_member in credits_struct.crew:
            if crew_member.job == 'Director':
                return crew_member.name
    return None


@udf(IntegerType())
def get_crew_size(credits_struct):
    """
    Get total number of crew members.
    
    Args:
        credits_struct: Credits struct containing crew array
        
    Returns:
        int: Number of crew members or 0
    """
    if credits_struct is not None and credits_struct.crew is not None:
        return len(credits_struct.crew)
    return 0


@udf(StringType())
def sort_pipe_separated(text_value, separator='|'):
    """
    Sort pipe-separated values alphabetically.
    
    Used for sorting genres alphabetically.
    
    Args:
        text_value: Pipe-separated string
        separator: Separator character (default: '|')
        
    Returns:
        str: Alphabetically sorted pipe-separated string or None
    """
    if text_value is not None and isinstance(text_value, str):
        values = text_value.split(separator)
        sorted_values = sorted(values)
        return separator.join(sorted_values)
    return None
