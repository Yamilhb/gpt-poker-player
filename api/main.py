from openai import OpenAI
import base64
import os
# import requests

api_key = os.getenv('POKER_GPT_KEY')
if api_key is None:
    raise ValueError("API_KEY no encontrada. Asegúrate de configurar esta variable de entorno.")

# Function to encode the image
def encode_image(image_path):
  with open(image_path, "rb") as image_file:
    return base64.b64encode(image_file.read()).decode('utf-8')

# Path to the image
image_path = "images/mano.png"

# Getting the base64 string
base64_image = encode_image(image_path)

# GPT-4 magic
pedido="Dame información que puedas extraer para yo realizar mi apuesta teniendo en cuenta que UTG raiseó u los dos siguientes foldearon. Te hago las siguientes preguntas enumeradas: 1.¿Puedes diferenciar el monto que apostó UTG? 2.¿Qué manos podrían ser más fuertes que la mía? 3.¿Qué debo hacer si a priori no conozco nada de ningún jugador?"
client = OpenAI(api_key=api_key)

response = client.chat.completions.create(
    model="gpt-4-turbo",
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
                   ],
               "name":"Ludio"}]
)
print(f"PREGUNTA: {pedido}\n")
print(f"RESPUESTA: {response.choices[0].message.content}")