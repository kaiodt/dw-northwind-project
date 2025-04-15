
      -- back compat for old kwarg name
  
  
  
        
            
	    
	    
            
        
    

    

    merge into "DW_GOLD"."dbo"."dim_products" as DBT_INTERNAL_DEST
        using "DW_GOLD"."dbo"."dim_products__dbt_tmp" as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.product_sk = DBT_INTERNAL_DEST.product_sk))

    
    when matched then update set
        "product_sk" = DBT_INTERNAL_SOURCE."product_sk","product_id" = DBT_INTERNAL_SOURCE."product_id","product_name" = DBT_INTERNAL_SOURCE."product_name","quantity_per_unit" = DBT_INTERNAL_SOURCE."quantity_per_unit","unit_price" = DBT_INTERNAL_SOURCE."unit_price","units_in_stock" = DBT_INTERNAL_SOURCE."units_in_stock","units_on_order" = DBT_INTERNAL_SOURCE."units_on_order","reorder_level" = DBT_INTERNAL_SOURCE."reorder_level","discontinued" = DBT_INTERNAL_SOURCE."discontinued","last_modified" = DBT_INTERNAL_SOURCE."last_modified","silver_loaded_at" = DBT_INTERNAL_SOURCE."silver_loaded_at","gold_loaded_at" = DBT_INTERNAL_SOURCE."gold_loaded_at","valid_from" = DBT_INTERNAL_SOURCE."valid_from","valid_to" = DBT_INTERNAL_SOURCE."valid_to","is_current" = DBT_INTERNAL_SOURCE."is_current"
    

    when not matched then insert
        ("product_sk", "product_id", "product_name", "quantity_per_unit", "unit_price", "units_in_stock", "units_on_order", "reorder_level", "discontinued", "last_modified", "silver_loaded_at", "gold_loaded_at", "valid_from", "valid_to", "is_current")
    values
        ("product_sk", "product_id", "product_name", "quantity_per_unit", "unit_price", "units_in_stock", "units_on_order", "reorder_level", "discontinued", "last_modified", "silver_loaded_at", "gold_loaded_at", "valid_from", "valid_to", "is_current")

;

  