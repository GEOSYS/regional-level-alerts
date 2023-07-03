import os
import logging
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse,FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.openapi.docs import get_swagger_ui_html
from http import HTTPStatus
import datetime
from dotenv import load_dotenv
from geosyspy import Geosys
from geosyspy.utils.constants import *
from pydantic import BaseModel
from enum import Enum

from regionallevelalerts.processor import RegionalLevelAlerts
from regionallevelalerts import utils

app = FastAPI(
    docs_url=None,
    title="RegionalLevelAlerts"+"Api",
    description= "Raise weather or vegetation alerts on a regional entity based on a defined parameter (weather or vegetation) and threshold"
    )
app.mount("/static", StaticFiles(directory="./api/files"), name="static")

@app.get("/docs", include_in_schema=False)
async def swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url="/openapi.json",
        title="RegionalLevelAlerts"+"Api",
        swagger_favicon_url="/static/favicon.svg"
    )


class WeatherAlertParameters(BaseModel):
    """
    Body for get_weather_alerts endpoint
    """
    startDate: datetime.date
    endDate: datetime.date
    threshold: float

class VegetationAlertParameters(BaseModel):
    """
    Body for get_vegetation_alerts endpoint
    """
    observationDate: datetime.date
    threshold: float

class AgriquestBlockCodes:
    """
    Recreate Block Codes enumeration dynamically, based on GeosysPy Enum AgriquestBlocks, to have values as string instead of numbers (i.e. blocks internal IDs)
    """
    codes: Enum = Enum('AgriquestBlockCodes', {member: member for member in AgriquestBlocks.__members__.keys()})
    codes.__doc__ = "Available AgriQuest Block codes"

class AgriquestWeatherTypeCodes:
    """
    Recreate Weather types enumeration dynamically, based on GeosysPy Enum AgriquestWeatherType, to have Weather Type codes instead of names
    """
    codes: Enum = Enum('AgriquestWeatherTypeCodes', {member: member for member in AgriquestWeatherType.__members__.keys()})
    codes.__doc__ = "Available AgriQuest Weather types"


logger = logging.getLogger()
logger.setLevel(logging.INFO)

load_dotenv()


@app.post("/regional-level-alerts/get_weather_alerts", tags=["Analytic Computation"])
async def get_weather_alerts(item: WeatherAlertParameters,
                             blockCode: AgriquestBlockCodes.codes,
                             weatherType: AgriquestWeatherTypeCodes.codes,
                             operator: utils.NumbersComparisonOperator):
    
    try:
        logging.info(f"get_weather_alerts: {blockCode} - {str(item.startDate)} - {str(item.endDate)} - {weatherType} - threshold: {item.threshold} - operator: {operator}")

        # Initialize the client
        API_CLIENT_ID = os.getenv('API_CLIENT_ID')
        API_CLIENT_SECRET = os.getenv('API_CLIENT_SECRET')
        API_USERNAME = os.getenv('API_USERNAME')
        API_PASSWORD = os.getenv('API_PASSWORD')
        client = RegionalLevelAlerts(API_CLIENT_ID, API_CLIENT_SECRET, API_USERNAME, API_PASSWORD, Env.PROD, Region.NA)

        # Get weather alerts
        geosysPyBlockEnumMember = utils.get_enum_member_from_name(AgriquestBlocks, blockCode.name)
        geosysPyWeatherTypeEnumMember = utils.get_enum_member_from_name(AgriquestWeatherType, weatherType.name)
        regional_level_alerts_df = client.get_weather_alerts(geosysPyBlockEnumMember,
                                                             item.startDate,
                                                             item.endDate,
                                                             geosysPyWeatherTypeEnumMember,
                                                             item.threshold,
                                                             operator)
        if regional_level_alerts_df is None:
            response = JSONResponse(content="get_weather_alerts: No data found")    
            response.headers["Content-Type"] = "application/json; charset=utf-8"
            return response

        # Save result DataFrame into a csv, upload it on AWS S3 and Azure Blob Storage, and return it
        regional_level_alerts_csv_file_name = utils.save_dataframe_to_temporary_csv(regional_level_alerts_df, "_weather")
        
        if utils.write_file_to_aws_s3(regional_level_alerts_csv_file_name):
           logging.info("File uploaded to AWS")
        if utils.write_file_to_azure_blob_storage(regional_level_alerts_csv_file_name):
           logging.info("File uploaded to Azure")
        
        return FileResponse(regional_level_alerts_csv_file_name, filename=regional_level_alerts_csv_file_name)

    except Exception as exc:
        raise HTTPException(status_code = HTTPStatus.INTERNAL_SERVER_ERROR, detail = str(exc))


@app.post("/regional-level-alerts/get_vegetation_alerts", tags=["Analytic Computation"])
async def get_vegetation_alerts(item: VegetationAlertParameters,
                                blockCode: AgriquestBlockCodes.codes,
                                operator: utils.NumbersComparisonOperator):

    try:
        logging.info(f"get_vegetation_alerts: {blockCode} - {str(item.observationDate)} - threshold: {item.threshold} - operator: {operator}")

        # Initialize the client
        API_CLIENT_ID = os.getenv('API_CLIENT_ID')
        API_CLIENT_SECRET = os.getenv('API_CLIENT_SECRET')
        API_USERNAME = os.getenv('API_USERNAME')
        API_PASSWORD = os.getenv('API_PASSWORD')
        client = RegionalLevelAlerts(API_CLIENT_ID, API_CLIENT_SECRET, API_USERNAME, API_PASSWORD, Env.PROD, Region.NA)

        # Get vegetation alerts
        geosysPyBlockEnumMember = utils.get_enum_member_from_name(AgriquestBlocks, blockCode.name)
        regional_level_alerts_df = client.get_vegetation_alerts(geosysPyBlockEnumMember,
                                                             item.observationDate,
                                                             item.threshold,
                                                             operator)
        if regional_level_alerts_df is None:
            response = JSONResponse(content="get_vegetation_alerts: No data found")    
            response.headers["Content-Type"] = "application/json; charset=utf-8"
            return response

        # Save result DataFrame into a csv, upload it on AWS S3 and Azure Blob Storage, and return it
        regional_level_alerts_csv_file_name = utils.save_dataframe_to_temporary_csv(regional_level_alerts_df, "_vegetation")
        
        if utils.write_file_to_aws_s3(regional_level_alerts_csv_file_name):
           logging.info("File uploaded to AWS")
        if utils.write_file_to_azure_blob_storage(regional_level_alerts_csv_file_name):
           logging.info("File uploaded to Azure")
        
        return FileResponse(regional_level_alerts_csv_file_name, filename=regional_level_alerts_csv_file_name)

    except Exception as exc:
        raise HTTPException(status_code = HTTPStatus.INTERNAL_SERVER_ERROR, detail = str(exc))
