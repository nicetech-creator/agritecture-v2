from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache

class ORM_Crop:
    @staticmethod
    def info():
        return {
            "name": "orm_crop",
            "description": "Crop Model Representation"
        }
    
    @staticmethod
    def input_type():
        return {
            
        }
    
    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        if input_name == "list crops":
            return fetch_all(
                "SELECT id, crop_type FROM crops ORDER BY id")    
        raise ValueError("Unexpected input: " + str(input_name))
    

    
