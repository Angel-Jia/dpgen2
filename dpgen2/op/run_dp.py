from dflow.python import (
    OP,
    OPIO,
    OPIOSign,
    Artifact,
    TransientError,
    FatalError,
)
import os, json, dpdata
from pathlib import Path
from typing import (
    Tuple, 
    List, 
    Set,
)
from dpgen2.utils.run_command import run_command
from dpgen2.utils.chdir import set_directory
from dpgen2.constants import(
    vasp_conf_name,
    vasp_input_name,
    vasp_pot_name,
    vasp_kp_name,
    vasp_default_log_name,
    vasp_default_out_data_name,
)
from dargs import (
    dargs, 
    Argument, 
    Variant, 
    ArgumentEncoder,
)

class RunDP(OP):
    r"""Execute a VASP task.

    A working directory named `task_name` is created. All input files
    are copied or symbol linked to directory `task_name`. The VASP
    command is exectuted from directory `task_name`. The
    `op["labeled_data"]` in `"deepmd/npy"` format (HF5 in the future)
    provided by `dpdata` will be created.

    """

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "type_map": List[str],
            "config" : dict,
            "confs" : Artifact(List[Path]),
            "model" : Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "log": Artifact(List[Path]),
            "labeled_data" : Artifact(list[Path]),
        })

    @OP.exec_sign_check
    def execute(
            self,
            ip : OPIO,
    ) -> OPIO:
        r"""Execute the OP.

        Parameters
        ----------
        ip : dict
            Input dict with components:
        
            - `config`: (`dict`) The config of vasp task. Check `RunVasp.vasp_args` for definitions.
            - `task_name`: (`str`) The name of task.
            - `task_path`: (`Artifact(Path)`) The path that contains all input files prepareed by `PrepVasp`.

        Returns
        -------
            Output dict with components:
        
            - `log`: (`Artifact(Path)`) The log file of VASP.
            - `labeled_data`: (`Artifact(Path)`) The path to the labeled data in `"deepmd/npy"` format provided by `dpdata`.
        
        Exceptions
        ----------
        TransientError
            On the failure of VASP execution. 
        """
        config = ip['config']
        confs = ip['confs']
        model_path = ip['model']
        type_map = ip['type_map']

        max_batch = config['max_batch']
        model_type_map = config['model_type_map']
        
        # ensure all elements in type_map are also in model_type_map
        assert len(set(type_map).intersection(set(model_type_map))) == len(type_map)

        from deepmd.infer import DeepPot
        dp = DeepPot(model_path)
        task_paths = []
        log_paths = []
        counter = 0
        
        # loop over list of MultiSystems
        for mm in confs:
            ms_model = dpdata.MultiSystems(type_map=model_type_map)
            ms_model.from_deepmd_npy(mm, labeled=False)
            
            ms = dpdata.MultiSystems(type_map=type_map)
            ms.from_deepmd_npy(mm, labeled=False)
            # loop over Systems in MultiSystems
            for ii in range(len(ms)):
                ss_model = ms_model[ii]
                ss = ms[ii]
                task_path, log_path = self.model_eval(ss_model, ss, dp, max_batch, counter)
                
                # task_names.append(task_name)
                task_paths.append(task_path)
                log_paths.append(log_path)
                counter += 1

        return OPIO({
            "log": log_paths,
            "labeled_data": task_paths
        })

    def model_eval(
        self,
        ss_model,
        ss,
        dp,
        max_batch,
        counter
    ):
        import numpy as np
        import sys
        
        task_name = 'sys.%06d' % counter
        task_path = Path(task_name)
        task_path.mkdir()
        
        log_path = task_path / 'output.log'
        
        # with open(log_path, 'w') as sys.stdout:
        #     sys.stderr = sys.stdout
            
        ss.to('deepmd/npy', task_path)
        
        coord_npy_path_list = list(task_path.glob('*/coord.npy'))
        assert len(coord_npy_path_list) == 1, coord_npy_path_list
        coord_npy_path = coord_npy_path_list[0]
        energy_npy_path = coord_npy_path.parent / 'energy.npy'
        force_npy_path = coord_npy_path.parent / 'force.npy'
        virial_npy_path = coord_npy_path.parent / 'virial.npy'
        
        nframe = ss_model.get_nframes()
        coord = ss_model['coords'].reshape([nframe, -1])
        cell = ss_model['cells'].reshape([nframe, -1])
        atype = ss_model['atom_types'].tolist()
        
        start_idx = 0
        end_idx = start_idx + max_batch
        
        energy = []
        force = []
        virial = []
        while start_idx < nframe:
            e, f, v = \
                dp.eval(coord[start_idx: end_idx, :], 
                        cell[start_idx: end_idx, :],
                        atype)
            
            energy.append(e)
            force.append(f)
            virial.append(v)
            # ss = dpdata.LabeledSystem()
            # ss.from_deepmd_npy(task_path)
            start_idx += max_batch
            end_idx += max_batch
        
        with open(energy_npy_path, 'wb') as f:
            np.save(f, np.concatenate(energy, axis=0).squeeze())
        with open(force_npy_path, 'wb') as f:
            np.save(f, np.concatenate(force, axis=0).reshape([nframe, -1]))
        with open(virial_npy_path, 'wb') as f:
            np.save(f, np.concatenate(virial, axis=0).reshape([nframe, -1]))
        
        log_path.write_text('done!')
        return task_path, log_path

