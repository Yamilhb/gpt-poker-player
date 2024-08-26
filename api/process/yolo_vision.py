import sys
from pathlib import Path
# Set the module paths
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from api.configuration.config import IMG_DIR
from PIL import ImageGrab
from screeninfo import get_monitors
from ultralytics import YOLO
import cv2
import numpy as np


# Load a pre-trained YOLOv10n model
model = YOLO("yolov10b.pt")
#model = YOLO("yolov10s.pt")
#model = YOLO("yolov10m.pt")
#model = YOLO("yolov9s.pt")
#model = YOLO("yolov8m.pt")


# Con lo siguientes nos damos cuenta que no reconoce las mesas de poker. Habrá que hacer un intenso fine tunning.
###############################
# # # Perform object detection on an image
# results = model(f"{IMG_DIR}/Mano_2024-05-12:23-14-41.png")
# 
# # Display the results
# results[0].show()

# Tamaño de la pantalla
monitor = get_monitors()[0]

# Veamos si podemos capturar nuestra pantalla.
###############################
x1=0
y1=0
x2=monitor.width
y2=monitor.height
con=0
xaux1 = 0
yaux1 = 0
xaux2 = 1
yaux2  = 1
while True:
    # Capturar la pantalla
    screen = np.array(ImageGrab.grab(bbox=(x1,y1,x2,y2)))  # Captura una parte de la pantalla

    # Convertir a RGB
    img_rgb = cv2.cvtColor(screen, cv2.COLOR_BGR2RGB)

    # Realizar la detección
    results = model(img_rgb)
    
    # Queremos centrar la captura de la pantalla en la mes
    # Obtener las coordenadas de la caja delimitadora (xmin, ymin, xmax, ymax)
    if len(results[0].boxes)>0:
        for box in results[0].boxes:
            print('PASO1')

            if (box.cls[0] == 13):
                print('PASO2',x1,y1,x1,y2)
                xyxy = box.xyxy.cpu().numpy().astype(float)[0]  # Convertir a NumPy array y a entero
                x1=x1 + xyxy[0] -2
                y1=y1 + xyxy[1] -2
                x2=x1 + (xyxy[2] - xyxy[0]) +3 
                y2=y1 + (xyxy[3] - xyxy[1]) +5
                # xaux1 = xyxy[0]
                # yaux1 = xyxy[1]
                # xaux2 = xyxy[2]
                # yaux2 = xyxy[3]
                # x2=x1+ (xyxy[2] - xyxy[0])
                # y2=y1+ (xyxy[3] - xyxy[1])
                con=0
                print(f'NUM OBJ: {len(results[0].boxes)}')
                print(f'CLASE: {box.cls}, coord: {box.xyxy}')
                break
            else:
                print('PASO3')
                con +=1 
    else:
        print('PASO4')
        con += 1
    if con>=5:
        print('PASO5')
        x1=0
        y1=0
        x2=monitor.width
        y2=monitor.height
    print(f'X1: {x1}, Y1: {y1}, X2: {x2}, Y2: {y2}')
    print(f'CCCCOOOON: {con}')

 
    # Renderizar los resultados
    annotated_frame = results[0].plot()  # Dibujar las detecciones sobre la imagen
    
    # Mostrar los resultados
    cv2.imshow('YOLO', annotated_frame)

    # Salir con la tecla 'q'
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

cv2.destroyAllWindows()


