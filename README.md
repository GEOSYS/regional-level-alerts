<div id="top"></div>
<!-- PROJECT SHIELDS -->
<!--
*** See the bottom of this document for the declaration of the reference variables
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->


<!-- PROJECT LOGO -->
<br />
<p align="center">
  <a href="https://github.com/GEOSYS">
    <img src="https://earthdailyagro.com/wp-content/uploads/2022/01/Logo.svg" alt="Logo" width="400" height="200">
  </a>

  <h1 align="center">Regional Level Alerts</h1>

  <p align="center">
    Learn how to use &ltgeosys/&gt platform capabilities in your own business workflow! Build your processor and learn how to run them on your platform.
    <br />
    <a href="https://earthdailyagro.com/"><strong>Who we are</strong></a>
    <br />
    <br />
    <a href="https://github.com/GEOSYS/GeosysPy/issues">Report Bug</a>
    ·
    <a href="https://github.com/GEOSYS/GeosysPy/issues">Request Feature</a>
  </p>
</p>

<div align="center">
</div>

<div align="center">
  
[![LinkedIn][linkedin-shield]][linkedin-url]
[![Twitter][twitter-shield]][twitter-url]
[![Youtube][youtube-shield]][youtube-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]

</div>


<!-- TABLE OF CONTENTS -->
<details open>
  <summary>Table of Contents</summary>
  
- [About The Project](#about-the-project)
- [Getting Started](#getting-started)
  * [Prerequisite](#prerequisite)
  * [Installation](#installation)
- [Usage](#usage)
  * [Run the example inside a Docker container](#run-the-example-inside-a-docker-container)
  * [Usage with Jupyter Notebook](#usage-with-jupyter-notebook)
- [Project Organization](#project-organization)
- [Resources](#resources)
- [Support development](#support-development)
- [License](#license)
- [Contact](#contact)
- [Copyrights](#copyrights)

</details>

<!-- ABOUT THE PROJECT -->
## About The Project

<p> The aim of this project is to help our customers valuing &ltgeosys/&gt platform capabilities to build their own analytic of interest. </p>

This directory exposes an example of code that will enable you to get weather or vegetation alerts on a regional entity based on a defined parameter (weather or vegetation) and threshold.

This directory allows you to run this example both through a notebook and as a local application on your machine. 
 

<p align="right">(<a href="#top">back to top</a>)</p>


<!-- GETTING STARTED -->
## Getting Started

### Prerequisite
 <p align="left">
Use of this project requires valids credentials from the &ltgeosys/&gt platform . If you need to get trial access, please register <a href=https://earthdailyagro.com/geosys-registration/>here</a>.
</p>

To be able to run this example, you will need to have following tools installed:

1. Install Conda: please install Conda on your computer. You can download and install it by following the instructions provided on the [official Conda website](https://conda.io/projects/conda/en/latest/user-guide/install/index.html)

2. Install Docker Desktop: please install Docker Desktop on your computer. You can download and install it by following the instructions provided on the [official Docker Desktop website](https://docs.docker.com/desktop/)

3. Install Jupyter Notebook: please install jupyter Notebook on your computer following the instructions provided on the [official Jupyter website](https://jupyter.org/install)

4. Install Git: please install Github on your computer. You can download and install it by visiting <a href=https://desktop.github.com/>here</a> and following the provided instructions


This package has been tested on Python 3.11.4.

<p align="right">(<a href="#top">back to top</a>)</p>


### Installation

To set up the project, follow these steps:

1. Clone the project repository:

    ```
    git clone https://github.com/GEOSYS/regional-level-alerts
    ```


2. Change the directory:

    ```
    cd regional-level-alerts
    ```


3. Fill the environment variable (.env)

Ensure that you populate the .env file with your Geosys APIs credentials. If you haven't acquired the credentials yet, please click [here](https://earthdailyagro.com/geosys-registration/) to obtain them.

   ```
   API_CLIENT_ID = <your client id>
   API_CLIENT_SECRET = <your client id>
   API_USERNAME = <your username>
   API_PASSWORD = <your password>

   AWS_ACCESS_KEY_ID = <...>
   AWS_SECRET_ACCESS_KEY = <...>
   AWS_BUCKET_NAME = <...>

   AZURE_ACCOUNT_NAME = <...>
   AZURE_BLOB_CONTAINER_NAME = <...>
   AZURE_SAS_CREDENTIAL = <...>
   ```

<p align="right">(<a href="#top">back to top</a>)</p>


<!-- USAGE -->
## Usage


### Usage with Jupyter Notebook

To use the project with Jupyter Notebook, follow these steps:


1. Create a Conda environment:

    To create a Conda environment, ensure first you have installed Conda on your computer. You can download and install it by following the instructions provided on the official Conda website. 

    
    ```
    conda create -y --name demo_regionallevelalerts
    ```


2. Activate the Conda environment:
    
    ```
    conda activate demo_regionallevelalerts

    ```
   
3. Install the project dependencies. You can do this by running the following command in your terminal:

    ```
    conda install -y pip
    pip install -r requirements.txt
    pip install ipykernel
    ```
4. Set up the Jupyter Notebook kernel for the project:

    ```
    python -m ipykernel install --user --name demo_regionallevelalerts --display-name regionallevelalerts
    ```
5. Open the example notebook (regionallevelalerts.ipynb) by clicking on it.



6. Select the "Kernel" menu and choose "Change Kernel". Then, select "regionallevelalerts" from the list of available kernels.


7. Run the notebook cells to execute the code example.

<p align="right">(<a href="#top">back to top</a>)</p>

### Run the example inside a Docker container

To set up and run the project using Docker, follow these steps:

1. Build the Docker image locally:
    
    ```
    docker build --tag demo_regionallevelalerts .
    ```

2. Run the Docker container:

    ```
    docker run -d --name demo_regionallevelalerts_container -p 8083:80 demo_regionallevelalerts
    ```

3. Access the API by opening a web browser and navigating to the following URL:
    
    ```
    http://127.0.0.1:8083/docs
    ```


This URL will open the Swagger UI documentation, click on the "Try it out" button under each POST endpoint and enter the request parameters and body


#### regional-level-alerts/get_weather_alerts endpoint:

Parameters:
  - Code of the Block, ex: "AMU_NORTH_AMERICA"
  - Weather Parameter, ex: "max-temperature"
  - Operator, ex: ">"

Body Example:
```json
{
  "startDate": "2022-01-01",
  "endDate": "2022-12-31",
  "threshold": 100
}
```

#### regional-level-alerts/get_vegetation_alerts endpoint:

Parameters:
  - Code of the Block, ex: "AMU_NORTH_AMERICA"
  - Operator, ex: ">"

Body Example:
```json
{
  "observationDate": "2022-12-31",
  "threshold": 0.55
}
```

<!-- PROJECT ORGANIZATION -->
## Project Organization


    ├── README.md         
    ├── notebooks          
    ├── requirements.txt    
    ├── environment.yml   
    │
    ├── setup.py           																								   
    ├───src                
    │   ├───main.py 
    │   ├───api
    │   │   ├── files
    │   │   │   └── favicon.svg
    │   │   ├── __init__.py
    │   │   └── api.py
    │   └───regionallevelalerts
    │       ├── __init__.py
    │       ├── processor.py
    │       └── utils.py      
    └── tests  

<p align="right">(<a href="#top">back to top</a>)</p>


<!-- RESOURCES -->
## Resources 
The following links will provide access to more information:
- [EarthDaily agro developer portal  ](https://developer.geosys.com/)
- [Pypi package](https://pypi.org/project/geosyspy/)

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- CONTRIBUTING -->
## Support development

If this project has been useful, that it helped you or your business to save precious time, don't hesitate to give it a star.

<p align="right">(<a href="#top">back to top</a>)</p>

## License

Distributed under the [GPL 3.0 License](https://www.gnu.org/licenses/gpl-3.0.en.html). 

<p align="right">(<a href="#top">back to top</a>)</p>

## Contact

For any additonal information, please [email us](mailto:sales@earthdailyagro.com).

<p align="right">(<a href="#top">back to top</a>)</p>

## Copyrights

© 2022 Geosys Holdings ULC, an Antarctica Capital portfolio company | All Rights Reserved.

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
<!-- List of available shields https://shields.io/category/license -->
<!-- List of available shields https://simpleicons.org/ -->
[contributors-shield]: https://img.shields.io/github/contributors/github_username/repo.svg?style=social
[contributors-url]: https://github.com/github_username/repo/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/github_username/repo.svg?style=plastic&logo=appveyor
[forks-url]: https://github.com/github_username/repo/network/members
[stars-shield]: https://img.shields.io/github/stars/qgis-plugin/repo.svg?style=plastic&logo=appveyor
[stars-url]: https://github.com/github_username/repo/stargazers
[issues-shield]: https://img.shields.io/github/issues/GEOSYS/GeosysPy/repo.svg?style=social
[issues-url]: https://github.com/github_username/repo/issues
[license-shield]: https://img.shields.io/github/license/GEOSYS/qgis-plugin
[license-url]: https://www.gnu.org/licenses/gpl-3.0.en.html
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=social&logo=linkedin
[linkedin-url]: https://www.linkedin.com/company/earthdailyagro/mycompany/
[twitter-shield]: https://img.shields.io/twitter/follow/EarthDailyAgro?style=social
[twitter-url]: https://img.shields.io/twitter/follow/EarthDailyAgro?style=social
[youtube-shield]: https://img.shields.io/youtube/channel/views/UCy4X-hM2xRK3oyC_xYKSG_g?style=social
[youtube-url]: https://img.shields.io/youtube/channel/views/UCy4X-hM2xRK3oyC_xYKSG_g?style=social
[language-python-shiedl]: https://img.shields.io/badge/python-3.9-green?logo=python
[language-python-url]: https://pypi.org/ 
[GitStars-shield]: https://img.shields.io/github/stars/GEOSYS?style=social
[GitStars-url]: https://img.shields.io/github/stars/GEOSYS?style=social
[CITest-shield]: https://img.shields.io/github/workflow/status/GEOSYS/GeosysPy/Continous%20Integration
[CITest-url]: https://img.shields.io/github/workflow/status/GEOSYS/GeosysPy/Continous%20Integration





