import open3d as o3d
import numpy as np

from utils import *
# from box_tx import invert_transformation_matrix


def invert_transformation_matrix(T):
    R = T[:3, :3]
    t = T[:3, 3]
    R_inv = R.T
    t_inv = -np.dot(R.T, t)
    T_inv = np.eye(4)
    T_inv[:3, :3] = R_inv
    T_inv[:3, 3] = t_inv
    return T_inv

def read_pcd_file(file_path):
    # Read the point cloud from the PCD file
    point_cloud = o3d.io.read_point_cloud(file_path)
    return point_cloud

def apply_transformation(point_cloud, transformation_matrix):
    # Apply the transformation matrix to the point cloud
    point_cloud.transform(transformation_matrix)
    return point_cloud

def visualize_point_clouds_with_origins(point_clouds, transformations):
    geometries = []

    # Add original point clouds and coordinate frames
    for i, point_cloud in enumerate(point_clouds):
        geometries.append(point_cloud)
        coordinate_frame = o3d.geometry.TriangleMesh.create_coordinate_frame(size=0.5)
        if i < len(transformations):
            coordinate_frame.transform(transformations[i])
        geometries.append(coordinate_frame)

    # Visualize all geometries
    o3d.visualization.draw_geometries(geometries)

def create_cube(width=1, height=1, depth=1):
    """
    Creates a 3D cube and visualizes it using Open3D.

    :param width: Width of the cube
    :param height: Height of the cube
    :param depth: Depth of the cube
    """
    # Create a mesh box (cube)
    cube = o3d.geometry.TriangleMesh.create_box(width=width, height=height, depth=depth)
    
    # Optionally, you can apply transformations to move or rotate the cube
    # For example, translate the cube to center it at (0, 0, 0)
    # cube.translate([-width / 2, -height / 2, -depth / 2])
    
    # Compute vertex normals for better visualization
    cube.compute_vertex_normals()
    
    return cube


def return_geometries():

    # Example usage:
    file_path1 = '/home/krish/digiflec/ciim_server/test_data/pcd/museum/museum/outsight1.pcd'
    file_path2 = '/home/krish/digiflec/ciim_server/test_data/pcd/museum/museum/outsight2.pcd'

    # Read the point clouds
    point_cloud1 = read_pcd_file(file_path1)
    point_cloud2 = read_pcd_file(file_path2)
    
    pcd_rotation = np.array([
                                    [0, -1, 0, 0],
                                    [1, 0, 0, 0],
                                    [0, 0, 1, 0],
                                    [0, 0, 0, 1]])
    

    pcd_rotation_inv = np.array([
                                    [0, 1, 0, 0],
                                    [-1, 0, 0, 0],
                                    [0, 0, 1, 0],
                                    [0, 0, 0, 1]])
    
    
    point_cloud1.transform(pcd_rotation)
    point_cloud2.transform(pcd_rotation)
    # Define a transformation matrix (e.g., a translation of (1, 2, 3))
    tx_string = """[[ 0.89012756  0.43314684  0.14162183 -1.05476715]
    [ 0.45092887 -0.88207911 -0.13638032  3.39223367]
    [ 0.06584896  0.18525725 -0.98048134 14.57950745]
    [ 0.          0.          0.          1.        ]]"""

    # print(tx_string)

    lidar2outsight2 = np.array([[0.000000005738, 0.950766265392, 0.309909284115, 0.000000004915],  
                    [0.030258791521, 0.309767156839, -0.950330793858, 0.000000603709],
                    [-0.999542057514, 0.009377479553, -0.028769034892, 2.517921924591],
                    [0., 0., 0., 1.]])
    lidar2outsight2[:3, :3] = lidar2outsight2[:3,:3].T
    lidar1outsight1 = np.array([[-0.000000007889, 0.987557828426, 0.157255813479, 0.000000000315], 
                    [0.068122684956, 0.156890496612, -0.985263705254, -0.000000019457], 
                    [-0.997676968575, 0.010712679476, -0.067275092006, 2.058652639389],
                    [0., 0., 0., 1.]])
    lidar1outsight1[:3, :3] = lidar1outsight1[:3,:3].T
    outsight1lidar1 = invert_transformation_matrix(lidar1outsight1)
    lidar2lidar1 = invert_transformation_matrix(string2array(tx_string))
    # lidar2lidar1 = string2array(tx_string)

    tx = np.dot(pcd_rotation, lidar2lidar1)
    tx = np.dot(tx, pcd_rotation_inv)
    lidar2lidar1 = tx
    outsight1_frame = o3d.geometry.TriangleMesh.create_coordinate_frame(size=0.5)
    outsight2lidar2 = invert_transformation_matrix(lidar2outsight2)
    lidar2outsight1 = np.dot(lidar1outsight1, lidar2lidar1)
    # outsight2lidar1 = np.dot(outsight2lidar2, lidar2lidar1)
    outsight2outsight1 = np.dot(lidar2outsight1, outsight2lidar2)

    calibration_matrix = outsight2outsight1
    print(calibration_matrix)
    outsight1_frame = o3d.geometry.TriangleMesh.create_coordinate_frame(size=0.5)
    outsight2_frame = o3d.geometry.TriangleMesh.create_coordinate_frame(size=0.5)
    lidar1_frame = o3d.geometry.TriangleMesh.create_coordinate_frame(size=0.5)
    lidar2_frame = o3d.geometry.TriangleMesh.create_coordinate_frame(size=0.5)

    point = np.array([[1,1,1]])

    outsight2_frame.transform(outsight2outsight1)
    lidar1_frame.transform(lidar1outsight1)
    lidar2_frame.transform(lidar2outsight1)


    # Apply the transformation to the second point cloud
    transformed_point_cloud1 = apply_transformation(point_cloud1, lidar1outsight1)
    transformed_point_cloud2 = apply_transformation(point_cloud2, lidar2outsight1)

    # Visualize both point clouds with their origins
    # visualize_point_clouds_with_origins([point_cloud1, transformed_point_cloud2, outsight2_frame], [np.eye(4), transformation_matrix])

    # geometries = [outsight1_frame, outsight2_frame, lidar1_frame, lidar2_frame, transformed_point_cloud1, transformed_point_cloud2]
    cube = create_cube()
    geometries = [outsight1_frame, lidar1_frame,lidar2_frame, point_cloud1, point_cloud2,  outsight2_frame]

    # o3d.visualization.draw_geometries(geometries)
    return geometries, outsight2outsight1

return_geometries()