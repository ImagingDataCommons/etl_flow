// Assumes that nexflow is invoked from within some appropriate virtual environment
process detect_tcia_collection_name_changes {
    """
    python3.9 ${launchDir}/preingestion/detect_tcia_collection_name_changes.py
    """
}

process revise_original_collections_metadata_idc_source_table {
    """
    python3.9 ${launchDir}/bq/generate_tables_and_views/original_collections_metadata_idc_source.py --spreadsheet_id 1fWG9Json963fDL8P9dPyzYEVqs11Nba6nnhZtrSJepQ
    """
}

process analysis_results_metadata_idc_source {
    """
    python3.9 ${launchDir}/bq/generate_tables_and_views/analysis_results_metadata_idc_source.py --spreadsheet_id 1Nu9uQNDOXBLUA9w8hp32b41cjPls6NOkjlgDwxAFgWw
    """
}

process analysis_results_descriptions {
    """
    python3.9 ${launchDir}/bq/generate_tables_and_views/analysis_results_descriptions.py
    """
}

workflow {
    detect_tcia_collection_name_changes | \
    revise_original_collections_metadata_idc_source_table | \
    analysis_results_metadata_idc_source | \
    analysis_results_descriptions
}