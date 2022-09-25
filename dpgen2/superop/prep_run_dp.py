from dflow import (
    InputParameter,
    OutputParameter,
    Inputs,
    InputArtifact,
    Outputs,
    OutputArtifact,
    Workflow,
    Step,
    Steps,
    upload_artifact,
    download_artifact,
    argo_range,
    argo_len,
    argo_sequence,
)
from dflow.python import(
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    Artifact,
    Slices,
)
from dpgen2.constants import (
    vasp_index_pattern,
)
from dpgen2.utils.step_config import normalize as normalize_step_dict
from dpgen2.utils.step_config import init_executor

import os
from typing import Set, List
from pathlib import Path
from copy import deepcopy

class PrepRunDP(Steps):
    def __init__(
            self,
            name : str,
            run_op : OP,
            run_config : dict = normalize_step_dict({}),
            upload_python_package : str = None,
    ):
        self._input_parameters = {
            "block_id" : InputParameter(type=str, value=""),
            "fp_config" : InputParameter(),
            "type_map" : InputParameter(),
            "inputs": InputParameter(),
        }
        self._input_artifacts = {
            "confs" : InputArtifact(),
            "dp_model": InputArtifact(optional=True)
        }
        # self._output_parameters = {
        #     "task_names": OutputParameter(),
        # }
        self._output_artifacts = {
            "logs": OutputArtifact(),
            "labeled_data": OutputArtifact(),
        }

        super().__init__(
            name=name,
            inputs=Inputs(
                parameters=self._input_parameters,
                artifacts=self._input_artifacts,
            ),
            outputs=Outputs(
                artifacts=self._output_artifacts,
            ),
        )
        
        self._keys = ['run-dp']
        
        ii = 'run-dp'
        self.step_keys[ii] = '--'.join(
            ["%s"%self.inputs.parameters["block_id"], ii + "-{{item}}"]
        )

        self = _prep_run_dp(
            self, 
            self.step_keys,
            run_op,
            run_config = run_config,
            upload_python_package = upload_python_package,
        )            

    @property
    def input_parameters(self):
        return self._input_parameters

    @property
    def input_artifacts(self):
        return self._input_artifacts

    @property
    def output_parameters(self):
        return self._output_parameters

    @property
    def output_artifacts(self):
        return self._output_artifacts

    @property
    def keys(self):
        return self._keys



def _prep_run_dp(
        prep_run_steps,
        step_keys,
        run_op : OP,
        run_config : dict = normalize_step_dict({}),
        upload_python_package : str = None,
):
    run_config = deepcopy(run_config)
    run_template_config = run_config.pop('template_config')
    run_executor = init_executor(run_config.pop('executor'))

    run_dp = Step(
        'run-dp',
        template=PythonOPTemplate(
            run_op,
            output_artifact_archive={
                "task_paths": None
            },
            python_packages = upload_python_package,
            **run_template_config,
        ),
        parameters={
            "type_map" : prep_run_steps.inputs.parameters['type_map'],
            "inputs": prep_run_steps.inputs.parameters['inputs'],
            "config" : prep_run_steps.inputs.parameters["fp_config"]
        },
        artifacts={
            "confs" : prep_run_steps.inputs.artifacts['confs'],
            "model": upload_artifact(run_config['model_path'])
        },
        key = step_keys['prep-dp'],
        executor = run_executor,
        **run_config,        
    )
    prep_run_steps.add(run_dp)

    # prep_run_steps.outputs.parameters["task_names"].value_from_parameter = prep_fp.outputs.parameters["task_names"]
    prep_run_steps.outputs.artifacts["logs"]._from = run_dp.outputs.artifacts["log"]
    prep_run_steps.outputs.artifacts["labeled_data"]._from = run_dp.outputs.artifacts["labeled_data"]

    return prep_run_steps


