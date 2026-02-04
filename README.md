CGMF_UQ
Author: Bill Hillman
----------------------
CGMF is a code which simulates prompt fission neutron and gamma emission from excited fission fragments right after scission.
CGMF main repository: https://github.com/lanl/CGMF
For the purposes of this project a fork of CGMF has been made which parametises gamma strength function 'magic numbers' and places them in gstrength_gdr_params.dat

The purpose of this repository is to provide a wrapper through which randomly perturbed CGMF calculations can be performed.
Perturbations are carried out by mainpulating .dat files within CGMF.
This project includes the following specific .dat files at the time of writing:
. gstrength_gdr_params.dat\n
. tkemodel.dat\n
. spinscallingmodel.dat\n
. rta.dat
. yamodel.dat (COMMING SOON)
. kcksyst.dat (COMMING SOON)
. deformations.dat (COMMING SOON)

This is done in two phases.
. Phase 1: Sensitivity A/B Testing
. Phase 2: Random Sampling (COMMING SOON)

Project Structure:

PROJECT_ROOT/
├── CGMF_Data_Default/           # Baseline .dat files
│   ├── deformations.dat
│   ├── gstrength_gdr_params.dat
│   ├── kcksyst.dat
│   ├── rta.dat
│   ├── spinscalingmodel.dat
│   ├── tkemodel.dat
│   ├── yamodel.dat
│   └── .....
│
├── Config/
│   ├── Default_Scale_Factors.json
│   ├── Parameter_Registry.yaml
│   └── Sensitivity_Coeff.yaml
│
├── POST_PROCESSING_SCRIPTS/
│   └── Post_Processing_V2.py
│
├── SLURM_SCRIPTS/              
│   └── submit_sensitivity.py
│
├── templates/
│   └── Sensitivity_Job_Template.sh
│
└── cgmf_uq/
    │
    ├── io/
    │   ├── dat_generator.py
    │   ├── dat_parser.py
    │   ├── generate_scale_factor_json.py
    │   ├── param_json_yaml_mapper.py
    │   └── FILE_PARSERS/
    │       ├── PARSE_deformations.py
    │       ├── PARSE_gstrength.py
    │       ├── PARSE_kcksyst.py
    │       ├── PARSE_rta.py
    │       ├── PARSE_spinscaling.py
    │       ├── PARSE_tkemodel.py
    │       └── PARSE_yamodel.py
    │
    ├── slurm/
    │   └── SLURM_Single_Job_Generator.py
    │
    └── workflow/
        └── indexing.py
