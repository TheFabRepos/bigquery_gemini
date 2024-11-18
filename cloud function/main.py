
import functions_framework
from flask import jsonify
import json
import os
import vertexai
from vertexai.generative_models import GenerativeModel, Image, Part, SafetySetting

import http.client
import typing
import urllib.request
import asyncio

eventLoop = asyncio.new_event_loop()
asyncio.set_event_loop(eventLoop)

@functions_framework.http

def list_url_and_prompt(request) -> str | tuple[dict]:
    """
    Extracts image URLs and corresponding text prompts from a request object.

    Args:
        request: The incoming request object containing a JSON payload.

    Returns:
        A list of dictionaries, where each dictionary contains an 'url' and 'text_prompt' key-value pair, 
        or an error message with a 400 status code if an exception occurs.
    """
    dict_url_and_prompt = {}
    list_dict = []
    request_json = request.get_json()
    calls = request_json["calls"]
    for i,call in enumerate(calls):
        print (f"{i} -> image_url: {str(call[0])} -> text_prompt: {str(call[1])}")
        list_dict.append({"url":str(call[0]), "text_prompt":str(call[1])})
    print(f"From list_url_and_prompt -> List dictionnary:{list_dict}")
    return list_dict

async def async_generate(uri, prompt, model) -> str:
  """
  Asynchronously generates text content based on an image and a prompt.

  Args:
      uri: The URI of the image.
      prompt: The text prompt to guide the generation.
      model: The model used for generating the content.

  Returns:
      The generated text content coming from gemini as as string 
  """
  image = Part.from_uri(
    mime_type="image/jpeg",
    uri=uri,
  )
  generation_config = {
    "max_output_tokens": 8192,
    "temperature": 1,
    "top_p": 0.95,
  }
  response = await model.generate_content_async(
      [image, f"""<url>{uri} </url> \n {prompt}"""],
      generation_config = generation_config,
      )
  print (f"From async_generate -> response: {response.text}")
  return response.text

async def wrapper(request):
    """
    Wraps the image-to-text generation process, handling initialization and error handling.

    Args:
        request: The incoming request object containing image URIs and prompts.

    Returns:
        A JSON string containing the generated text replies, or an error message with a 400 status code.
    """
    try:
        project_id = os.environ.get("PROJECT_ID")
        region = os.environ.get("REGION")
        vertexai.init(project=project_id, location=region)
        model = GenerativeModel("gemini-1.5-flash-002") # model is hard coded here but can easily be passed as a parameter
        list_url_prompt = list_url_and_prompt(request)

        get_responses = [async_generate(f'{item['url']}', f'{item['text_prompt']}', model) for item in list_url_prompt]
        results = await asyncio.gather (*get_responses)
        return json.dumps({"replies": results})
    except Exception as e:
        return json.dumps({"errorMessage": str(e)}), 400

def run_it(request) -> str | tuple[str, int]:
    """
    Entry-point function triggered by a Cloud Function URL.

    Args:
        request: The incoming request object from the Cloud Function trigger. It contains the records sent by BigQuery when calling the Cloud Function

    Returns:
        A JSON string containing the generated text replies or an error message with a 400 status code.
    """
    print(f"From run_it -> request: {request.get_json()["calls"]}")
    return eventLoop.run_until_complete(wrapper(request))
