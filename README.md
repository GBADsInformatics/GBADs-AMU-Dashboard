# GBADs-AMU-Dashboard

<h2>Overview</h2>

This repository contains the code related to estimates around the antimicrobial usage (AMU) and resistance (AMR) and how that is related to the animal health loss envelope (AHLE). 
This contains a global view as well as country-specific case studies.

<h2>Origin</h2>

Developed based on the AMRU case study, originally part of the [GBADsLiverpool repository](https://github.com/GBADsInformatics/GBADsLiverpool).
Built upon the AHLE Dashboard framework.

<h2>Repository Structure</h2>

```
.
├── Dash App/          			# Directory for Dash App files
│   ├── assets/			    	# Directory for assets within dashboard
│   ├── data/			     	# Directory for processed data used within dashboard
│   ├── lib/			     	# Directory for python libraries used within dashboard
│   ├── AMU_Dash_UI.py  		# Python script for Dash dashborad Excel data to YAML
│   ├── Dockerfile		     	# Dockerfile used to created webpage for dash dashboard
├── Data Processing/          	# Directory for Data Processing files
│   ├── processed_data/			# Directory for data files that have been processed
│   ├── raw_data/			    # Directory for raw data files
│   ├── 1_prepare_data.py  		# Python script for processing data
├── LICENSE		            	# License
├── README.md               	# Project documentation
```

<h2>Contributors</h2>
- Kristen McCaffrey
- Justin Replogle
- Kassy Raymond 

---

