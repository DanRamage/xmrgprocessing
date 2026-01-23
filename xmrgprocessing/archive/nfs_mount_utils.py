import os
import logging.config
import subprocess

def test_docker_host_volume(mount_point):
    logger = logging.getLogger()
    files = os.listdir(mount_point)
    logger.debug(f"Files at the mount: {files}")
    logger.info(f"Checking if {mount_point} is writable.")
    # Try accessing the mount point
    try:
        test_file = os.path.join(mount_point, '.nfs_test_file')
        with open(test_file, 'w') as f:
            f.write('test')
        os.remove(test_file)
        logger.info(f"{mount_point} is a writable.")
        return True
    except Exception as e:
        logger.error(f"Failed to write test file to {mount_point}: {e}")
    return False

def check_mount_exists(mount_point):
    logger = logging.getLogger()
    # Check if the mount point exists
    logger.info(f"Checking if {mount_point} is a valid mount point.")
    if not os.path.ismount(mount_point):
        logger.error(f"{mount_point} is not a valid mount point.")
        return False
    return test_docker_host_volume(mount_point)


def mount_nfs(server, remote_path, local_mount_point):
    logger = logging.getLogger()

    # Create the local mount point directory if it doesn't exist
    #os.makedirs(local_mount_point, exist_ok=True)

    # Mount the NFS path
    mount_command = f"mount -t nfs {server}:{remote_path} {local_mount_point}"
    try:
        subprocess.run(mount_command, shell=True, check=True)
        logger.info(f"Successfully mounted {server}:{remote_path} to {local_mount_point}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to mount {server}:{remote_path} to {local_mount_point}: {e}")
    return False