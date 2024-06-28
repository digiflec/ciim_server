import open3d as o3d
import numpy as np
import threading
import time
from utils import objects2boxes, load_synced_data_from_json, transform_frames
from open3d_viz import return_geometries
class BoundingBoxVisualizer:
    def __init__(self, initial_geometries, loaded_frames, max_boxes=100, check_interval=0.1):
        self.loaded_frames = loaded_frames
        self.max_boxes = max_boxes
        self.check_interval = check_interval
        self.initial_geometries = initial_geometries
        self.frame_iter = iter(loaded_frames)
        self.bbox_geometries = []
        self.vis = o3d.visualization.VisualizerWithKeyCallback()
        self.running = True

    def create_bounding_box(self, corners):
        """
        Creates a bounding box as an Open3D LineSet.
        
        :param corners: The corners of the bounding box.
        :param color: The color of the bounding box.
        :return: An Open3D LineSet object representing the bounding box.
        """
        lines = [
            [0, 1], [0, 3], [0, 4],
            [1, 2], [1, 5],
            [2, 3], [2, 6],
            [3, 7],
            [4, 5], [4, 7],
            [5, 6],
            [6, 7]
        ]

        line_set = o3d.geometry.LineSet()
        line_set.points = o3d.utility.Vector3dVector(corners)
        line_set.lines = o3d.utility.Vector2iVector(lines)

        return line_set

    def update_bounding_box(self, line_set, corners):
        """
        Updates the corners of an existing bounding box.
        
        :param line_set: The LineSet object representing the bounding box.
        :param corners: The new corners of the bounding box.
        """
        line_set.points = o3d.utility.Vector3dVector(np.asarray(corners))

    def initialize_visualizer(self):
        """
        Initializes the visualizer and adds initial bounding boxes.
        """
        self.vis.create_window()
        
        for geometries in self.initial_geometries:
            self.vis.add_geometry(geometries)

        # Initialize bounding box geometries
        for _ in range(self.max_boxes):
            bbox = self.create_bounding_box(np.zeros((8, 3)))
            self.bbox_geometries.append(bbox)
            self.vis.add_geometry(bbox)
        
        # Register the key callback to update the visualizer
        self.vis.register_key_callback(ord("N"), self.update_visualizer)

    def update_visualizer(self, vis):
        """
        Callback function to update the visualizer.
        """
        if not self.loaded_frames:
            print('no more frames')
            return True  # No frames to process, continue looping
            
        frame = self.loaded_frames.pop(0)
        bbox_idx = 0
        print('loading frame')
        for cache_key, boxes in frame.items():
            
            # print('cache_key:', cache_key)
            # print('boxes: ', len(boxes))
            for box in boxes:
                if bbox_idx >= self.max_boxes:
                    print("Warning: More boxes than expected, increase max_boxes.")
                    break
                corners = box.corners()
                if cache_key == 'cache0':
                    self.update_bounding_box(self.bbox_geometries[bbox_idx], corners.T)
                    self.bbox_geometries[bbox_idx].paint_uniform_color([1, 0, 0])
                elif cache_key == 'cache1':
                    self.update_bounding_box(self.bbox_geometries[bbox_idx], corners.T)
                    self.bbox_geometries[bbox_idx].paint_uniform_color([0, 1, 0])
                bbox_idx += 1

        # Hide remaining boxes
        for i in range(bbox_idx, self.max_boxes):
            self.update_bounding_box(self.bbox_geometries[i], np.zeros((8, 3)))

        # Update the visualizer
        for bbox in self.bbox_geometries:
            vis.update_geometry(bbox)
        vis.poll_events()
        vis.update_renderer()

        return True  # Signal to continue the loop

    def run(self):
        """
        Runs the visualizer.
        """
        self.initialize_visualizer()
        self.vis.run()
        self.vis.destroy_window()

# Example usage:
# Assume `loaded_frames` is a list of frames, each containing bounding boxes
loaded_frames_dict = load_synced_data_from_json('synced_data.json')  # Populate this with actual data
geometries, calibration_matrix = return_geometries()

calibration_dict = {'sensor1_sensor0': calibration_matrix}
# print(loaded_frames_dict)
frames = transform_frames(loaded_frames_dict, calibration_dict)
# print(frames[1])
visualizer = BoundingBoxVisualizer(geometries, frames)
visualizer.run()
