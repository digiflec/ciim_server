from datetime import datetime
from pyquaternion import Quaternion
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from data_classes import Box
from scipy.spatial.transform import Rotation as R
import json
def record_to_frame(record):
    
    if not record or len(record)==0:
        return None
    if 'Data' not in record[0]:
        return None
    
    lines = record[0]['Data'].decode('utf-8').splitlines()
    frame = lines[0].split(',')

    frame_dict = {
        'frame_Count': int(frame[1]),
        'time_s': frame[2],
        'formatted_time': frame[3],
        'time': datetime.fromisoformat(frame[3]),
        'number_of_objects': int(frame[4]),
        'zone_bindings_len': int(frame[5]),
        'objects': [],
        }


    objects = lines[1:1+frame_dict['number_of_objects']]
    obj = dict()
    for object in objects:
        object = object.split(',')
        obj = {
            'frame_count': int(object[1]),
            'obj_id': int(object[2]),
            'object_class': object[3],
            'pos_x': float(object[4]),
            'pos_y': float(object[5]),
            'pos_z': float(object[6]),
            'dim_x': float(object[7]),
            'dim_y': float(object[8]),
            'dim_z': float(object[9]),
            'speed_mph': float(object[10]),
            'bearing_degrees': float(object[11]),
        }

        frame_dict['objects'].append(obj)
    return frame_dict



def objects2boxes(objects):
    boxes = []
    for obj in objects:
        yaw_angle = obj['bearing_degrees'] * np.pi / 180
        yaw_quaternion = Quaternion(axis=[0, 0, 1], angle=yaw_angle)
        center = [obj['pos_x'], obj['pos_y'], obj['pos_z']]
        size = [obj['dim_x'], obj['dim_y'], obj['dim_z']]
        box = Box(center, size, yaw_quaternion, velocity = [obj['speed_mph'], 0, 0]) # Convert this to 3D velocity
        boxes.append(box)
    return boxes


def transform_box(box, transformation_matrix):
    """
    Transforms the center and rotation of a 3D bounding box using a 4x4 transformation matrix.

    :param center: The center of the bounding box in frame 1.
    :param rotation: The rotation of the bounding box in frame 1 as a quaternion.
    :param transformation_matrix: The 4x4 transformation matrix from frame 2 to frame 1.
    :return: The new center and rotation of the bounding box in frame 2.
    """
    # Transform the center
    center_homogeneous = np.append(box.center, 1)
    new_center_homogeneous = np.dot(transformation_matrix, center_homogeneous)
    new_center = new_center_homogeneous[:3]

    # Transform the rotation

    rotation_matrix = box.orientation.rotation_matrix
    rotation_part = transformation_matrix[:3, :3]
    new_rotation_matrix = np.dot(rotation_part, rotation_matrix)

    r = R.from_matrix(new_rotation_matrix)
    quat = r.as_quat()


    new_rotation = Quaternion(quat[3], quat[0], quat[1], quat[2])

    
    box.center = new_center
    box.orientation = new_rotation

    return box



def transform_boxes_list(boxes, calibration_matrix):
    # Use ProcessPoolExecutor to run the transformations in parallel
    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(transform_box, box, calibration_matrix)
            for box in boxes
        ]

        # Collect the results
        boxes = [future.result() for future in futures]
    return boxes 

def string2array(data_string):

    lines = data_string.replace("[", "").replace("]", "")

    lines = lines.strip().splitlines()
    data = []

    for line in lines:
        cleaned_line = line.strip().split()

        data.append([float(x) for x in cleaned_line])

    array = np.array(data)
    return array

def rx2rpy(rx):
    r = R.from_matrix(rx)

    # Convert to Euler angles (roll, pitch, yaw)
    roll, pitch, yaw = r.as_euler('xyz', degrees=True)
    return (roll, pitch, yaw)

def calib2string(calibration_matrix):
    rx = calibration_matrix[:3,:3]
    tx = calibration_matrix[:3, 3]
    rpy = rx2rpy(rx)
    print(f"X: {tx[0]:.2f} meters, Y: {tx[1]:.2f}, Z:{tx[2]:.2f}, Roll: {rpy[0]:.2f} degrees, Pitch: {rpy[1]:.2f} degrees, Yaw: {rpy[2]:.2f} degrees")

def transform_point(point, transformation_matrix):
    # Ensure the point is in homogeneous coordinates
    homogeneous_point = np.append(point, 1)
    
    # Apply the transformation
    transformed_homogeneous_point = np.dot(transformation_matrix, homogeneous_point)
    # Convert back to 3D coordinates
    transformed_point = transformed_homogeneous_point[:3] / transformed_homogeneous_point[3]
    
    return transformed_point

def load_synced_data_from_json(filename: str):
    with open(filename, 'r') as f:
        frames = json.load(f)

    return frames


def transform_frames(frames_dict, calibration_dict):
    frames = []

    for frame in frames_dict:
        new_frame = {}
        for cache_key, cache in frame.items():
            
            boxes = objects2boxes(cache['objects'])
            if cache_key == 'cache0':
                new_frame['cache0'] = boxes
            if cache_key == 'cache1':
                new_frame['cache1'] = transform_boxes_list(boxes, calibration_dict['sensor1_sensor0'])
        frames.append(new_frame)
    return frames


calibration_string = """[[ 0.2314198  -0.         -0.97285398  0.        ]
 [ 0.          1.         -0.          0.        ]
 [ 0.97285398  0.          0.2314198  -2.01845491]
 [ 0.          0.          0.          1.        ]]"""

calibration_matrix = string2array(calibration_string)



# for frame in loaded_frames:
#     print("Frame:")
#     geometries_with_boxes = geometries
#     for cache_key, boxes in frame.items():
#         if cache_key == 'cache0':
#             for box in boxes:
#                 print('cache0:', box)
#                 geometries_with_boxes.append(create_bounding_box(box.corners(), np.eye(4)))
#         if cache_key == 'transformed':
#             for box in boxes:
#                 print('transformed:', box)
#                 geometries_with_boxes.append(create_bounding_box(box.corners(), np.eye(4), [0,1,0]))
#     o3d.visualization.draw_geometries(geometries_with_boxes)





