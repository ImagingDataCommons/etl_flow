params.current_version = 19
params.previous_version = 18
baseDir = "/pycharm/etl_flow/tmp/pycharm_project_936"
projectDir = "/pycharm/etl_flow/tmp/pycharm_project_936"
params.detect_tcia_collection_name_changes_script = "$projectDir/preingestion/detect_tcia_collection_name_changes.py"
params.venv_path = "$projectDir/venv"

process detect_tcia_collection_name_changes {
    debug true

    input:
    path detect_tcia_collection_name_changes_script

    """
    source ${params.venv_path}/bin/activate
//    pip list
    export SETTINGS_MODULE=settings
    export SECURE_LOCAL_PATH=../secure_files/etl
    export PYTHONUNBUFFERED=1
    python3.9 ${detect_tcia_collection_name_changes_script}
    deactivate
    """
}

workflow {
  detect_tcia_collection_name_changes(params.detect_tcia_collection_name_changes_script)
}