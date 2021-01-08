"""
Create light for each point in the pointcloud
and control its property either by the point attribute
or by referencing the pxrlightarrays parameters.

Generated lightarrays can be either pxr standard light nodes
such as 'PxrSphereLight' inside the current houdini session
or to save on disk as delayed rib archive.
"""
import os
import time
import hou
import pdg
import pdgd
import json
import logging
import subprocess
import platform
import binascii
import archive_template
from threading import Thread
from Queue import Queue
from collections import namedtuple
from glob import iglob
from itertools import islice


logger = logging.getLogger('pxrlightarrays')
logging.basicConfig(level=logging.INFO, format='%(name)s:%(message)s')

WORK_ITEMS_KEY = pdgd.PropertyNames.WorkItems

light_type_dict = {0: 'PxrRectLight', 1: 'PxrDiskLight',
                   2: 'PxrSphereLight', 3: 'PxrCylinderLight'}
rect_specific_parms = ['lightColorMap', 'colorMapGamma', 'colorMapSaturation']
transform_parms = {'pxrrectlight': [('sx', 'scalex', 0), ('sy', 'scaley', 0)],
                   'pxrcylinderlight': [('sx', 'length', 0),
                                        ('sy', 'scalex', 0),
                                        ('sz', 'scaley', 0)],
                   'all': [('tx', 'P', 0), ('ty', 'P', 1), ('tz', 'P', 2),
                           ('rx', 'rotation', 0), ('ry', 'rotation', 1),
                           ('rz', 'rotation', 2), ('scale', 'pscale', 0)]}
parm_type_dict = {'int': [('', 0)], 'float': [('', 0)],
                  'color': [('r', 0), ('g', 1), ('b', 2)],
                  'vector': [('x', 0), ('y', 1), ('z', 2)]}


def read_json():
    """Read the json that stores the pxrlightarrays parameter names."""
    current_dir = os.path.dirname(__file__)
    json_file = os.path.join(current_dir, 'configs', 'node_parms.json')
    with open(json_file, 'r') as file:
        node_parms = json.load(file)
    return node_parms
# end read_json


def select_pointcloud(node):
    """Select a proper pointcloud."""
    hou.ui.displayMessage('No pointcloud found, select a proper pointcloud',
                          severity=hou.severityType.ImportantMessage, title='pxrlightarrays')
    new_pointcloud_path = hou.ui.selectNode(title='select a pointcloud',
                                            node_type_filter=hou.nodeTypeFilter.Sop)
    if not new_pointcloud_path: return
    node.parm('ptcPath').set(new_pointcloud_path)
# end select_pointcloud


def init_attributes(node, pointcloud_geo):
    """
    Create the specific pxrlightarrays attributes if they are not
    available on pointcloud.
    """
    attribs = ['lightColor', 'pscale', 'N', 'scalex', 'scaley']
    point_attribs = pointcloud_geo.pointAttribs()
    available_attribs = {point_attrib.name() for point_attrib in point_attribs
        if point_attrib.name() in attribs}

    if not available_attribs.issuperset(attribs):
        pointcloud_node = node.parm('ptcPath').evalAsNode()
        pointcloud_node_parent = pointcloud_node.parent()
        pointcloud_node_attrib = pointcloud_node_parent.createNode('attribcreate::2.0',
                                                                   'init_lightarrays_attributes')
        pointcloud_out_null = pointcloud_node_parent.createNode('null', 'OUT_source_pointcloud')

        node_parms = read_json()
        pointcloud_node_attrib.setColor(hou.Color(0.9, 0.1, 0.1))
        pointcloud_node_attrib.setPosition(pointcloud_node.position())
        pointcloud_node_attrib.move([0, -1.1])
        pointcloud_node_attrib.setInput(0, pointcloud_node)
        pointcloud_node_attrib.parm('numattr').set(5)
        pointcloud_node_attrib.setParms(node_parms['attrib_create']['vals'])
        pointcloud_node_attrib.setParmExpressions(node_parms['attrib_create']['expr'])

        pointcloud_out_null.setColor(hou.Color(0.48, 0.48, 0.48))
        pointcloud_out_null.setPosition(pointcloud_node_attrib.position())
        pointcloud_out_null.move([0, -1.1])
        pointcloud_out_null.setInput(0, pointcloud_node_attrib)
        pointcloud_out_null.setDisplayFlag(True)
        pointcloud_out_null.setRenderFlag(True)

        node.parm('ptcPath').set(pointcloud_out_null.path())
# end init_attributes


class LightArraysInteractive(object):
    """A class for generating lightarrays inside the current houdini session."""

    def __init__(self, node):
        self.node = node
        self.node_name = self.node.name()
        self.node_path = self.node.path()
        self.pointcloud = self.node.parm('ptc_path').evalAsNode()
        self.pointcloud_path = self.pointcloud.path()
        self.light_type = self.node.evalParm('lightType')
        self.time_range = self.node.evalParm('trange')
        frame_range = namedtuple('frame_range', ['start', 'end', 'step'])
        frange = frame_range(*self.node.evalParmTuple('f'))
        self.frame_range = range(frange.start, frange.end + 1, frange.step)
        self.node_parms = read_json()
    # end __init__

    def create_light(self, subnet, index):
        """Creating a single light from incoming parameters."""
        light_name = light_type_dict[self.light_type].lower()
        light_array = subnet.createNode('%s::22' % light_name, '%s_%04i' % (light_name, index))
        light_array.setDisplayFlag(False)

        if light_name in transform_parms:
            light_array.setParmExpressions({'%s' % x[0]:
                                            'point("%s", %i, "%s", %i)' % (self.pointcloud_path, index, x[1], x[2])
                                            for x in transform_parms[light_name]})
        light_array.setParmExpressions({'%s' % x[0]:
                                        'point("%s", %i, "%s", %i)' % (self.pointcloud_path, index, x[1], x[2])
                                        for x in transform_parms['all']})

        lgt_enable_parm = light_array.parm('light_enable')
        if self.time_range:
            lgt_enable_parm.set(0)
        else:
            lgt_enable_parm.setExpression('ch("%s/light_enable")' % self.node_path)

        for parm_type in self.node_parms['pxrlightarrays'].iterkeys():
            for parm_name in self.node_parms['pxrlightarrays'][parm_type]:
                if self.light_type != 0 and parm_name in rect_specific_parms: continue
                self.set_parameters(parm_type, parm_name, index, light_array)
    # end create_light

    def set_parameters(self, *args):
        """
        Set either by point expression or channel referencing based on override state.
        """
        parm_type, parm_name, index, light = args
        override_parm = self.node.parm(parm_name + 'Override')
        if override_parm and override_parm.eval():
            if parm_type == 'string':
                node_parm = light.parm(parm_name)
                node_parm.revertToDefaults()
                node_parm.set('`points("%s", %i, "%s")`' % (self.pointcloud_path, index, parm_name))
            else:
                light.setParmExpressions({'%s%s' % (parm_name, x[0]):
                                          'point("%s", %i, "%s", %i)' % (self.pointcloud_path, index, parm_name, x[1])
                                          for x in parm_type_dict[parm_type]})
        else:
            if parm_type == 'string':
                node_parm = light.parm(parm_name)
                node_parm.revertToDefaults()
                node_parm.set('`chs("%s/%s")`' % (self.node_path, parm_name))
            else:
                light.setParmExpressions({'%s%s' % (parm_name, x[0]):
                                          'ch("%s/%s%s")' % (self.node_path, parm_name, x[0])
                                          for x in parm_type_dict[parm_type]})
    # end set_parameters

    def generate(self):
        """Create light node(s) for each point of the pointcloud."""
        start_time = time.time()
        if not self.node.evalParm('ptcPath'):
            select_pointcloud(self.node)
            return

        init_attributes(self.node, self.pointcloud.geometry())

        time_range_parm = self.node.evalParm('trange')
        python_sop_node = self.node.parm('python_path').evalAsNode()
        max_num_points = python_sop_node.evalParm('current_total_points')
        if time_range_parm:
            point_counts_list = []
            for current_frame in self.frame_range:
                pointcloud_geo = self.pointcloud.geometryAtFrame(current_frame)
                if not pointcloud_geo: continue
                init_attributes(self.node, pointcloud_geo)
                point_counts_list.append(len(pointcloud_geo.iterPoints()))
            max_num_points = max(point_counts_list)
        if not max_num_points:
            select_pointcloud(self.node)
            return

        self.node.setUserData('total_number_lights', str(max_num_points))
        self.destroy_subnet()

        obj_path = self.node.parent()
        subnet_node = obj_path.createNode('subnet', self.node_name + '_network')
        subnet_node.setUserData('pxrlightarrays', self.node_name)
        subnet_node.setColor(hou.Color(0.85, 0.8, 0.5))
        subnet_node.setPosition(self.node.position())
        subnet_node.move([0, -1.1])
        subnet_node.setNextInput(self.node)

        with hou.InterruptableOperation('Performing', 'Generating lightarrays',
                                        open_interrupt_dialog=True) as operation:
            for point in range(max_num_points):
                percent = point / float(max_num_points)
                operation.updateLongProgress(percent, 'Generating lightarrays: %s' % point)
                self.create_light(subnet_node, point)

        subnet_node.layoutChildren()
        self.node.parm('light_enable').set(1)
        if time_range_parm:
            self.node.setUserData('current_number_lights', str(0))
            python_sop_node.cook(force=True)

        logger.info('Lightarrays generated in: {}s'.format(time.time() - start_time))
    # end generate

    def get_subnet(self):
        """Check if light network subnet exist."""
        subnet_type = hou.nodeType('Object/subnet')
        subnets = subnet_type.instances()
        if not subnets: return

        for subnet in subnets:
            if subnet.userData('pxrlightarrays') == self.node_name:
                return subnet
    # end get_subnet

    def change_parm_override(self, parm_name, parm_type):
        """Change parm override behavior."""
        subnet = self.get_subnet()
        if not subnet: return

        lights = subnet.children()
        for index, light in enumerate(lights):
            self.set_parameters(parm_type, parm_name, index, light)
    # end change_parm_override

    def light_type_callback(self):
        """A callback for changing lightType."""
        subnet = self.get_subnet()
        if not subnet: return

        self.generate()
    # end light_type_callback

    def correct_light_connections(self, current_total_points):
        """Updating lights based on point counts."""
        subnet = self.get_subnet()
        if not subnet: return

        light_enable_parm = self.node.parm('light_enable')
        total_number_lights = int(self.node.userData('total_number_lights'))

        if total_number_lights >= current_total_points:
            lights = subnet.children()
            for idx, light in enumerate(lights):
                light_enable_parm = light.parm('light_enable')
                light_enable_parm.deleteAllKeyframes()
                if idx < current_total_points:
                    light_enable_parm.setExpression('ch("%s/light_enable")' % self.node_path)
                else:
                    light_enable_parm.set(0)
            self.node.setUserData('current_number_lights', str(current_total_points))
        elif light_enable_parm.eval():
            light_enable_parm.set(0)
            hou.ui.displayMessage('lightarrays will not be available outside of the frame range',
                                  severity=hou.severityType.Warning, title='pxrlightarrays')
    # end correct_light_connections

    def destroy_subnet(self):
        """Destroy lightarrays subnet if its exist."""
        subnet = self.get_subnet()
        if not subnet: return

        self.node.parm('light_enable').set(1)
        subnet.destroy()
    # end destroy_subnet
# end LightArraysInteractive


class LightArraysArchive(object):
    """
    Base class for generating lightarrays archive on disk.
    """

    def __init__(self, node):
        self.node = node
        self.pointcloud = self.node.parm('ptc_path').evalAsNode()
        self.light_type = self.node.evalParm('lightType')
        self.name_prefix = light_type_dict[self.light_type].lower()
        self.method_parm = self.node.evalParm('exec_method')
        self.saveto_parm = self.node.parm('saveto')
        self.time_range = self.node.evalParm('trange')
        frame_range = namedtuple('frame_range', ['start', 'end', 'step'])
        self.named_range = frame_range(*self.node.evalParmTuple('f'))
        self.frame_range = range(self.named_range.start,
                                 self.named_range.end + 1,
                                 self.named_range.step)
    # end __init__

    def point_attr_description(self, current_point, point_attrib_names):
        """Setup archive description for a single point in the pointcloud."""
        self.description['lightId'] = self.generate_random_hash()
        self.description['lightName'] = '{}_{:04d}'.format(self.name_prefix, current_point.number())
        temp_var = current_point.floatListAttribValue('lightTransformationMatrix')
        self.description['lightTransformationMatrix'] = ' '.join(str(elem) for elem in temp_var)

        if self.override_parms:
            for override_parm in self.override_parms:
                parm_type, parm_name = override_parm[0], override_parm[1]
                if parm_name in point_attrib_names:
                    if parm_type == 'int':
                        temp_var = current_point.intAttribValue(parm_name)
                        self.description[parm_name] = str(temp_var)
                    elif parm_type == 'float':
                        temp_var = current_point.floatAttribValue(parm_name)
                        self.description[parm_name] = str(temp_var)
                    elif parm_type == 'color' or parm_type == 'vector':
                        temp_var = current_point.floatListAttribValue(parm_name)
                        temp_var = [str(elem) for elem in temp_var]
                        self.description[parm_name] = ' '.join(temp_var)
                    else:
                        temp_var = current_point.stringAttribValue(parm_name)
                        self.description[parm_name] = temp_var
            if self.name_prefix == 'pxrrectlight':
                rect_light_map = archive_template.extra_rect_light.format(**self.description)
                self.description['rect_lightMap'] = rect_light_map
        return self.description
    # end point_attr_description

    @staticmethod
    def write_archive_to_disk(archive, saveto_path):
        """Write a lightarrays archive to disk."""
        dir_path, file_name = os.path.split(saveto_path)
        output_temp = os.path.join(dir_path, 'temp_' + file_name)

        with open(output_temp, 'w') as file:
            file.write(archive)

        # convert ascii to binary archive
        args = ['catrib', '-binary', '-gzip', '-o', saveto_path, output_temp]
        temp_var = True if platform.system() == 'Windows' else False
        subprocess.call(args, shell=temp_var)
        os.remove(output_temp)
    # end write_archive_to_disk

    def generate_description(self, points, point_attrib_names):
        """Constructing a generator for retrieving pointcloud data to make archive."""
        with hou.InterruptableOperation('Generating lightarrays archive',
                                        open_interrupt_dialog=True) as sub_operation:
            for point in points:
                percent = point.number() / float(len(points))
                sub_operation.updateProgress(percent)
                full_description = self.point_attr_description(point, point_attrib_names)
                yield archive_template.light.format(**full_description)
    # end generate_description

    def make_archive(self, current_frame):
        """Make and save archive for given current frame if pointcloud data exist."""
        pointcloud_geo = self.pointcloud.geometryAtFrame(current_frame)
        if not pointcloud_geo: return

        init_attributes(self.node, pointcloud_geo)
        point_attribs = pointcloud_geo.pointAttribs()
        point_attrib_names = {point_attrib.name() for point_attrib in point_attribs}
        points = pointcloud_geo.points()
        archive_data = self.generate_description(points, point_attrib_names)

        archive = '##RenderMan RIB\nversion 3.04999995\n'
        lights_archive = ''.join(archive_data)
        archive += lights_archive

        return archive
    # end make_archive

    def init_archive(self):
        """
        Create delayed archive for generating large number of lights for
        deferred rendering.
        """
        if not self.node.evalParm('ptcPath'):
            select_pointcloud(self.node)
            return

        if not self.saveto_parm.eval():
            newSavetoPath = hou.ui.selectFile(title='Save the archive(s) to',
                                              pattern='*.rib',
                                              chooser_mode=hou.fileChooserMode.Write)
            if not newSavetoPath: return
            self.saveto_parm.set(newSavetoPath)

        python_sop_node = self.node.parm('python_path').evalAsNode()
        max_num_points = python_sop_node.evalParm('current_total_points')
        if not self.time_range and not max_num_points:
            select_pointcloud(self.node)
            return

        node_parms = read_json()
        self.override_parms = []
        self.description = dict()
        self.description['lightType'] = light_type_dict[self.light_type]
        for parm_type in node_parms['pxrlightarrays'].iterkeys():
            for parm_name in node_parms['pxrlightarrays'][parm_type]:
                override_parm = self.node.parm(parm_name + 'Override')
                if override_parm and override_parm.eval():
                    self.override_parms.append((parm_type, parm_name))
                if parm_type == 'int' or parm_type == 'float':
                    temp_var = self.node.evalParm(parm_name)
                    self.description[parm_name] = str(temp_var)
                elif parm_type == 'color' or parm_type == 'vector':
                    temp_var = self.node.parmTuple(parm_name).eval()
                    temp_var = [str(elem) for elem in temp_var]
                    self.description[parm_name] = ' '.join(temp_var)
                else:
                    temp_var = self.node.evalParm(parm_name)
                    self.description[parm_name] = temp_var
        rect_light_map = archive_template.extra_rect_light.format(**self.description)
        self.description['rect_lightMap'] = rect_light_map if self.name_prefix == 'pxrrectlight' else ''

        return True
    # end init_archive

    def destroy_archive(self):
        """Remove the archives of the disk and its dependencies."""
        if not self.saveto_parm.eval(): return
        user_confirm = hou.ui.displayConfirmation('Do you want to remove cached archive(s)?',
                                                  severity=hou.severityType.Warning,
                                                  title='pxrlightarrays')
        if not user_confirm: return

        delayed_archive = self.find_node(parent=hou.node('/shop'),
                                        node_type='pxrdelayedreadarchive::22')
        if delayed_archive:
            geo_archive = self.find_node(parent=hou.node('/obj'),
                                        node_type='geo', archive=delayed_archive)
            if geo_archive:
                geo_archive.destroy()
            delayed_archive.destroy()

        with hou.InterruptableOperation('Performing', 'Removing archive(s)',
                                        open_interrupt_dialog=True) as operation:
            if self.time_range:
                for current_frame in self.frame_range:
                    percent = current_frame / float(len(self.frame_range))
                    operation.updateLongProgress(percent, 'Frame: %s' % current_frame)
                    saveto_parm = self.saveto_parm.evalAtFrame(current_frame)
                    if os.path.exists(saveto_parm):
                        os.remove(saveto_parm)
            else:
                operation.updateLongProgress(1, 'Frame: %s' % hou.frame())
                if os.path.exists(self.saveto_parm.eval()):
                    os.remove(self.saveto_parm.eval())
    # end destroy_archive

    def read_delayed_archive(self):
        """
        Create or set delayedreadarchive shop and dummy geo
        to reference the procedural.
        """
        shop_path = hou.node('/shop')
        delayed_archive = self.find_node(parent=shop_path, node_type='pxrdelayedreadarchive::22')
        if not delayed_archive:
            delayed_archive = shop_path.createNode('pxrdelayedreadarchive::22',
                                                   '{}'.format(self.node.name() + '_delayedarchive'))
            delayed_archive.parm('file').set(self.saveto_parm.rawValue())
            delayed_archive.moveToGoodPosition()

        obj_path = hou.node('/obj')
        geo_archive = self.find_node(parent=obj_path, node_type='geo', archive=delayed_archive)
        if not geo_archive:
            geo_archive = obj_path.createNode('geo', '{}'.format(self.node.name() + '_readarchive'))
            geo_archive.parm('shop_geometrypath').set(delayed_archive.path())
            geo_archive.parm('categories').set('`chs("%s/categories")`' % self.node.path())
            geo_archive.setColor(hou.Color(1, 0.976, 0.666))
            geo_archive.setPosition(self.node.position())
            geo_archive.move([3, 0])
    # end read_delayed_archive

    def find_node(self, **kwargs):
        """Find a specific node in the node graph."""
        for child in kwargs['parent'].children():
            if child.type().name() == kwargs['node_type']:
                if kwargs['node_type'] == 'geo':
                    shop_geometrypath_parm = child.evalParm('shop_geometrypath')
                    if shop_geometrypath_parm == kwargs['archive'].path():
                        return child
                elif kwargs['node_type'] == 'pxrdelayedreadarchive::22':
                    file_parm_rvalue = child.parm('file').rawValue()
                    if self.saveto_parm.rawValue() == file_parm_rvalue:
                        return child
    # end find_node

    @staticmethod
    def generate_random_hash():
        """Generate an unique id for each lights in the archive."""
        rand_hash = '%s-%s-%s-%s-%s' % (binascii.hexlify(os.urandom(4)),
                                        binascii.hexlify(os.urandom(2)),
                                        binascii.hexlify(os.urandom(2)),
                                        binascii.hexlify(os.urandom(2)),
                                        binascii.hexlify(os.urandom(6)))
        return rand_hash
    # end generate_random_hash
# end LightArraysArchive


class InProcessArchive(LightArraysArchive):
    """
    A subclass for generating lightarrays archive running on the
    current houdini session.
    """

    def __init__(self, node, frame_range=None):
        super(InProcessArchive, self).__init__(node)
        if frame_range:
            self.frame_range = range(*frame_range)
    # end __init__

    def queue_loop(self, queue):
        """Create a queue loop to write/convert incoming archive in a separate thread."""
        while True:
            archive, saveto_path = queue.get()
            super(InProcessArchive, self).write_archive_to_disk(archive, saveto_path)
            queue.task_done()
    # end queue_loop

    def generate(self):
        """Generate delayed archive."""
        start_time = time.time()
        successed = super(InProcessArchive, self).init_archive()
        if not successed: return

        write_queue = Queue(maxsize=0)
        write_thread = Thread(target=self.queue_loop, args=(write_queue,))
        write_thread.daemon = True
        write_thread.start()

        with hou.InterruptableOperation('Writing to disk...', 'Frame',
                                        open_interrupt_dialog=True) as operation:
            if self.time_range:
                for current_frame in self.frame_range:
                    percent = current_frame / float(len(self.frame_range))
                    operation.updateLongProgress(percent, 'Frame: %s' % current_frame)
                    archive = super(InProcessArchive, self).make_archive(current_frame)
                    if not archive: continue
                    saveto_path = self.saveto_parm.evalAtFrame(current_frame)
                    write_queue.put((archive, saveto_path))
                write_queue.join()
            else:
                current_frame = hou.frame()
                operation.updateLongProgress(1, 'Frame: %s' % current_frame)
                archive = super(InProcessArchive, self).make_archive(current_frame)
                if not archive: return
                saveto_path = self.saveto_parm.evalAtFrame(current_frame)
                super(InProcessArchive, self).write_archive_to_disk(archive, saveto_path)

        super(InProcessArchive, self).read_delayed_archive()
        logger.info('Archive(s) generated in: {}sec'.format(time.time() - start_time))
    # end generate
# end InProcessArchive


class PDGInterruptableOperation(hou.InterruptableOperation):
    """
    A subclass for customizing pdg graph state once
    user interrupt the process.
    """

    def __init__(self, pdg_context, *args, **kwargs):
        super(PDGInterruptableOperation, self).__init__(*args, **kwargs)
        self.pdg_context = pdg_context
    # end __init__

    def __exit__(self, *args):
        if args[2]:
            self.pdg_context.cancelCook()
        return super(PDGInterruptableOperation, self).__exit__(*args)
    # end __exit__
# end PDGInterruptableOperation


class PDGOutOfProcessArchive(LightArraysArchive):
    """A subclass for generating lightarrays archive via PDG."""

    @staticmethod
    def graph_state_callback(pdg_context, pdgd_object):
        """A callback for self.retrieving current state of the graph."""
        states = ['cooked', 'cooking', 'total']
        state_dict = dict.fromkeys(states, 0)
        pdg_states = [pdg.workItemState.CookedSuccess, pdg.workItemState.Cooking]
        work_item_ids = frozenset(pdgd_object.object[WORK_ITEMS_KEY])
        for work_item_id in work_item_ids:
            work_item = pdg_context.graph.workItemById(work_item_id)
            for state_name, pdg_state in zip(states, pdg_states):
                if work_item.state == pdg_state:
                    state_dict[state_name] += 1
        state_dict['total'] = len(work_item_ids)
        return state_dict
    # end graph_state_callback

    def assign_range(self, process, num_processes):
        """Divide frame range based on number of allocated processes."""
        process_len_range = len(self.frame_range) / float(num_processes)
        if process_len_range < 1:
            hou.ui.displayMessage('Number of processes cannot exceed the number of frames.',
                                  severity=hou.severityType.Warning, title='pxrlightarrays')
            return

        from_index = process * process_len_range
        to_index = (process + 1) * process_len_range
        sliced_range = list(islice(self.frame_range, from_index, to_index))
        frame_range = map(int, (sliced_range[0], sliced_range[-1] + self.named_range.step,
                                self.named_range.step))
        return frame_range
    # end assign_range

    def execute_graph(self):
        """
        Initialize work items and set required attributes for further
        processing on the network chain.
        """
        start_time = time.time()
        hou.hipFile.saveAsBackup()
        current_hipfile = hou.hipFile.path()
        dir, filename = os.path.split(current_hipfile)
        backup_dir = os.environ.get('HOUDINI_BACKUP_DIR', os.path.join(dir, 'backup'))
        basename, _ = os.path.splitext(filename)
        hipfile_pattern = os.path.join(backup_dir, basename + '*')
        backup_hipfile = max(iglob(hipfile_pattern), key=os.path.getmtime)

        topnet_node = self.node.parm('topnet__archive').evalAsNode().displayNode()
        topnet_node.dirtyAllTasks(False)
        topnet_node.executeGraph(block=True)
        pdg_context = topnet_node.getPDGGraphContext()
        pdg_context_name = topnet_node.getPDGGraphContextName()

        data_layer = pdgd.util.get_local_data_layer()
        pdgd_object = pdgd.util.PDGDObject('pdg/%s' % pdg_context_name, data_layer)

        num_processes = self.node.evalParm('maxprocs')
        pdg_node = self.node.parm('pyprocessor_path').evalAsNode().getPDGNode()
        with PDGInterruptableOperation(
            pdg_context, 'Cooking PDG Graph', open_interrupt_dialog=True) as operation:
            with pdg_node.injectStaticItems() as item_holder:
                for process in range(num_processes):
                    proc_frame_range = self.assign_range(process, num_processes)
                    if not proc_frame_range: return
                    new_item = item_holder.addWorkItem(index=process)
                    new_item.setFloatAttrib('start_time', start_time)
                    new_item.setIntAttrib('framerange', proc_frame_range)
                    new_item.setStringAttrib('hipfile', backup_hipfile)
                    new_item.setStringAttrib('hdapath', self.node.path())

            topnet_node.executeGraph()
            while pdg_context.cooking:
                state_dict = self.graph_state_callback(pdg_context, pdgd_object)
                if not state_dict['total']: continue
                percent = state_dict['cooked'] / float(state_dict['total'])
                operation.updateLongProgress(percent, 'Cooked {cooked:02d}/{total:02d}'
                    ' work items, active {cooking:02d}'.format(**state_dict))

        super(PDGOutOfProcessArchive, self).read_delayed_archive()
    # end execute_graph
# end PDGOutOfProcessArchive
