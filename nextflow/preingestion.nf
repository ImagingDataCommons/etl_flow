// Assumes that nexflow is invoked from within some appropriate virtual environment
process detect_tcia_collection_name_changes {
    """
    python3.9 ${launchDir}/preingestion/detect_tcia_collection_name_changes.py
    """
}

workflow {
    detect_tcia_collection_name_changes()
}