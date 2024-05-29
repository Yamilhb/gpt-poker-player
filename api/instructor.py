import sys
from pathlib import Path
# Set the module paths
sys.path.append(str(Path(__file__).resolve().parent.parent))
print(sys.path)

from openai import OpenAI

import matplotlib
matplotlib.use('TkAgg')  # Usar la backend TkAgg, que es interactiva
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import time
import base64
import os
# import requests
from api.configuration.config import IMG_DIR

# Path to the image
image_path = f"{IMG_DIR}/"
user_poker_name="pokeados"

api_key = os.getenv('POKER_GPT_KEY')
if api_key is None:
    raise ValueError("API_KEY n0t found. Save your API key in a environment variable!")

client = OpenAI(api_key=api_key)

# Function to encode the image
def encode_image(image_path):
  with open(image_path, "rb") as image_file:
    return base64.b64encode(image_file.read()).decode('utf-8')


# GPT-4 magic

pedido=f"You are a profesio0nal poker player called {user_poker_name}. Tell me 2 things: 1º what else do you need to make a move. 2º What move would you do."

for im in os.listdir(image_path):
    if im.split(".")[-1]=="png":
        # Getting the base64 string
        base64_image = encode_image(f"{image_path}{im}")

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user",
                    "content": [
                        {
                            "type": "text", "text": pedido
                            },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{base64_image}"
                            }
                            }
                        ]
                        }]
        )
        
        # Instructors answer
        print(f"RESPUESTA: {response.choices[0].message.content}")
        # Show the image
        imag = mpimg.imread(f"{image_path}{im}")
        plt.imshow(imag)
        plt.axis('off')  # Ocultar los ejes
        plt.show()
        #time.sleep(5)
        #plt.close()