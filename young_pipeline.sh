#!/bin/bash
BASE_DIR=$(dirname "$(realpath "$0")")
LOG_FILE="$BASE_DIR/pipeline.log"
CONFIG_FILE="$BASE_DIR/config.yaml"

get_yaml_value() {
    local key=$1
    local file=$2
    yq .$key $file | tr -d '"'
}

should_skip_step() {
    local step=$1
    yq '.skip_steps[]' "$CONFIG_FILE" | grep -q "$step"
}

delete_directory_if_exists() {
    local dir=$1
    if [ -d "$dir" ]; then
        echo ""
        echo "[Directory $dir already exists. Deleting it to avoid conflicts.]"
        rm -rf "$dir"
    fi
}

MY_CRDS_PATH=$(get_yaml_value 'crds_path' "$CONFIG_FILE")
MY_CRDS_SERVER_URL=$(get_yaml_value 'crds_server_url' "$CONFIG_FILE")
export CRDS_PATH=$MY_CRDS_PATH
export CRDS_SERVER_URL=$MY_CRDS_SERVER_URL

UNCAL_PATH=$(get_yaml_value 'uncal_path' "$CONFIG_FILE")
WISP_NPROC=$(get_yaml_value 'wisp_nproc' "$CONFIG_FILE")

echo ""
echo "################################"
echo "#                              #"
echo "# JWST data reduction pipeline #"
echo "#                              #"
echo "################################"
echo ""

if [ -f "$LOG_FILE" ]; then
    echo "[Existing pipeline.log file deleted]"
    echo ""
    rm "$LOG_FILE"
fi

if ! should_skip_step "download_uncal_references"; then
    echo "« Downloading references for uncal.fits files »"
    echo "  ¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯  "
    crds bestrefs --files ${UNCAL_PATH}/jw*uncal.fits --sync-references=1
else
    echo "[Download uncal references skipped]"
fi

if ! should_skip_step "stage1"; then
    delete_directory_if_exists "$BASE_DIR/output/stage1_output"
    echo ""
    echo "==================="
    echo " Pipeline: stage 1 "
    echo "==================="
    python "$BASE_DIR/utils/pipeline_stage1.py" --output_dir "$BASE_DIR/output/stage1_output"
else
    echo "[Pipeline Stage 1 skipped]"
fi

if ! should_skip_step "fnoise_correction"; then
    echo ""
    echo "« Correcting 1/f noise »"
    echo "  ¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯  "
    python "$BASE_DIR/utils/remstriping_update_parallel.py" --runall --output_dir "$BASE_DIR/output/stage1_output"
else
    echo "[1/f noise correction skipped]"
fi

if ! should_skip_step "download_rate_references"; then
    echo ""
    echo "« Downloading references for rate.fits files »"
    echo "  ¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯  "
    crds bestrefs --files $BASE_DIR/output/stage1_output/jw*rate.fits --sync-references=1
else
    echo "[Download rate references skipped]"
fi

if ! should_skip_step "stage2"; then
    delete_directory_if_exists "$BASE_DIR/output/stage2_output"
    echo ""
    echo "===================="
    echo " Pipeline - stage 2 "
    echo "===================="
    python "$BASE_DIR/utils/pipeline_stage2.py" --input_dir "$BASE_DIR/output/stage1_output" --output_dir "$BASE_DIR/output/stage2_output"
else
    echo "[Pipeline Stage 2 skipped]"
fi

if ! should_skip_step "wisp_subtraction"; then
    echo ""
    echo "« Subtracting wisps from exposures »"
    echo "  ¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯  "
    python "$BASE_DIR/utils/subtract_wisp.py" --files $BASE_DIR/output/stage2_output/jw*cal.fits --wisp_dir "$BASE_DIR/utils/wisp-templates" --suffix "_final" --nproc "$WISP_NPROC"
else
    echo "[Wisp subtraction skipped]"
fi

if ! should_skip_step "background_subtraction"; then
    echo ""
    echo "« Subtracting background from exposures »"
    echo "  ¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯  "
    python "$BASE_DIR/utils/bkg_sub_parallel.py" --input_dir "$BASE_DIR/output/stage2_output" --output_dir "$BASE_DIR/output/stage2_output"
else
    echo "[Background subtraction skipped]"
fi

if ! should_skip_step "download_cal_references"; then
    echo ""
    echo "« Downloading references for cal.fits files »"
    echo "  ¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯  "
    crds bestrefs --files $BASE_DIR/output/stage2_output/jw*cal_final.fits --sync-references=1
else
    echo "[Download cal references skipped]"
fi

if ! should_skip_step "stage3"; then
    delete_directory_if_exists "$BASE_DIR/output/stage3_output"
    echo ""
    echo "===================="
    echo " Pipeline - stage 3"
    echo "===================="
    python "$BASE_DIR/utils/pipeline_stage3.py" --input_dir "$BASE_DIR/output/stage2_output" --output_dir "$BASE_DIR/output/stage3_output"
else
    echo "[Pipeline Stage 3 skipped]"
fi

echo ""
echo "===================="
echo " Pipeline completed "
echo "===================="
