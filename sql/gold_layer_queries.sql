# Querying the Star Schema in the Gold Layer
analytics_df = spark.sql("""
    SELECT 
        d.clean_drug_name AS drug_name,
        r.reaction_term AS reported_reaction,
        COUNT(f.fact_key) AS total_adverse_events
    FROM gold_fact_adverse_event f
    JOIN gold_dim_drug d ON f.drug_key = d.drug_key
    JOIN gold_dim_reaction r ON f.reaction_key = r.reaction_key
    GROUP BY d.clean_drug_name, r.reaction_term
    ORDER BY total_adverse_events DESC
""")