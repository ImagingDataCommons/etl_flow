# About

The queries in this folder correspond to the views used by the IDC portal to
flatten the DICOM content corresponding to DICOM Segmentation objects and DICOM SR
TID 1500 content (limited subset of functionality).

The views can be applied to the DICOM metadata BigQuery tables extracted using standard
DICOM metadata extraction process in Google Healthcare.

These views were confirmed to be functional (although, no comprehensive verification
of accuracy has been done) when run against the metadata extracted for the following
"original" TCIA collections and also analysis results ("3rd party") corresponding to
those collections (a.k.a. IDC "wave0" of content):

TCGA-UCEC, TCGA-SARC, TCGA-THCA, TCGA-STAD, TCGA-READ, TCGA-LIHC, TCGA-LGG, TCGA-HNSC, TCGA-ESCA, TCGA-COAD, TCGA-CESC, TCGA-BRCA, LIDC-IDRI, ISPY1, TCGA-PRAD, TCGA-OV, TCGA-LUSC, TCGA-KIRP, TCGA-LUAD, TCGA-KIRC, TCGA-KICH, TCGA-GBM, TCGA-BLCA, QIN-HEADNECK

Note that content of the collection corresponds to their content as extracted from
TCIA circa June 2020.

Location of the views in Google BigQuery corresponding to the content in this folder:

* `idc-dev-etl:canceridc_data.segmentations`
* `idc-dev-etl:canceridc_data.measurement_groups`
* `idc-dev-etl:canceridc_data.quantitative_measurements`
* `idc-dev-etl:canceridc_data.qualitative_measurements`
