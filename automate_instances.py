from googleapiclient import discovery
from datetime import datetime
from time import sleep

def delete_instance(compute, project, zone, name):
    return compute.instances().delete(
        project=project,
        zone=zone,
        instance=name).execute()

def create_instance(compute, project, zone, name):
    # Get the latest Debian Jessie image.
    source_disk_image = 'https://www.googleapis.com/compute/v1/projects/us-gm-175021/global/images/scp'
    machine_type = "zones/{0}/machineTypes/f1-micro".format(zone)
    startup_script = 'gs://scp_ds/boot_up.sh'

    # Configure the machine
    # machine_type = "zones/%s/machineTypes/n1-standard-1" % zone
    # image_url = "http://storage.googleapis.com/gce-demo-input/photo.jpg"
    # image_caption = "Ready for dessert?"

    config = {
        'name': name,
        'machineType': machine_type,

        # Specify the boot disk and the image to use as a source.
        'disks': [
            {
                'boot': True,
                'autoDelete': True,
                'initializeParams': {
                    'sourceImage': source_disk_image,
                }
            }
        ],

        # Specify a network interface with NAT to access the public
        # internet.
        'networkInterfaces': [{
            'network': 'https://www.googleapis.com/compute/v1/projects/us-gm-175021/global/networks/default',
            'subnetwork': 'https://www.googleapis.com/compute/v1/projects/us-gm-175021/regions/us-east1/subnetworks/default',
            'name': 'nic0',
            'accessConfigs': [
                {'type': 'ONE_TO_ONE_NAT', 'name': 'external-nat', 'networkTier': 'PREMIUM'}
            ]
        }],
        #
        # # Allow the instance to access cloud storage and logging.
        'serviceAccounts': [{
            'email': 'default',
            'scopes': [
                'https://www.googleapis.com/auth/devstorage.read_write',
                'https://www.googleapis.com/auth/logging.write'
            ]
        }],

        # Metadata is readable from the instance and allows you to
        # pass configuration from deployment scripts to instances.
        'metadata': {
            'items': [{
                # Startup script is automatically executed by the
                # instance upon startup.
                'key': 'startup-script-url',
                'value': startup_script
            }]
        }
    }

    return compute.instances().insert(
        project=project,
        zone=zone,
        body=config).execute()

if __name__ == '__main__':
    compute = discovery.build('compute', 'v1')
    project = 'us-gm-175021'
    zone = 'us-east1-b'

    max_num_instances = 20
    instance_limit = 60*60
    sleep_mins = .5
    instance_num = 1
    instances = {}
    while True:

        if len(instances) < max_num_instances:
            instance_name = 'scp-{:06d}'.format(instance_num)
            create_instance(compute, project, zone, instance_name)
            instances[instance_name] = datetime.now()
            instance_num += 1
            print('Created Instance {0}'.format(instance_name))

        for name, created_date in instances.items():
                if (datetime.now() - created_date).total_seconds() > instance_limit:
                    delete_instance(compute, project, zone, name)
                    print('Deleted Instance {0}'.format(name))

        sleep(60*sleep_mins)
