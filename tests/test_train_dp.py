import os
import numpy as np
import unittest

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
    S3Artifact,
    argo_range
)
from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    Artifact
)

import time, shutil, json
from typing import Set, List
from pathlib import Path

from context import dpgen2
from dpgen2.op.run_dp_train import RunDPTrain
from dpgen2.op.prep_dp_train import PrepDPTrain
from dpgen2.flow.train_dp import steps_train


class MockPrepDPTrain(PrepDPTrain):
    @OP.exec_sign_check
    def execute(
            self,
            ip : OPIO,
    ) -> OPIO:
        template = ip['template_script']
        numb_models = ip['numb_models']
        ofiles = []
        osubdirs = []

        for ii in range(numb_models):
            jtmp = template
            jtmp['seed'] = ii
            subdir = Path(f'task.{ii:04d}') 
            subdir.mkdir(exist_ok=True, parents=True)
            fname = subdir / 'input.json'
            with open(fname, 'w') as fp:
                json.dump(jtmp, fp, indent = 4)
            osubdirs.append(str(subdir))
            ofiles.append(fname)

        op = OPIO({
            "task_subdirs" : osubdirs,
            "train_scripts" : ofiles,
        })
        return op


class MockRunDPTrain(RunDPTrain):
    @OP.exec_sign_check
    def execute(
            self,
            ip : OPIO,
    ) -> OPIO:
        script = ip['train_script']
        work_dir = Path(ip['task_subdir'])
        init_model = Path(ip['init_model'])
        init_data = ip['init_data']
        iter_data = ip['iter_data']

        script = Path(script).resolve()
        init_model = init_model.resolve()
        init_model_str = str(init_model)
        init_data = [ii.resolve() for ii in init_data]
        iter_data = [ii.resolve() for ii in iter_data]
        init_data_str = [str(ii) for ii in init_data]
        iter_data_str = [str(ii) for ii in iter_data]

        with open(script) as fp:
            jtmp = json.load(fp)        
        data = []
        for ii in sorted(init_data_str):
            data.append(ii)
        for ii in sorted(iter_data_str):
            data.append(ii)
        jtmp['data'] = data
        with open(script, 'w') as fp:
            json.dump(jtmp, fp, indent=4)

        cwd = os.getcwd()
        work_dir.mkdir(exist_ok=True, parents=True)
        os.chdir(work_dir)

        oscript = Path('input.json')
        if not oscript.exists():
            from shutil import copyfile
            copyfile(script, oscript)
        model = Path('model.pb')
        lcurve = Path('lcurve.out')
        log = Path('log')

        assert(init_model.exists())        
        with log.open("w") as f:
            f.write(f'init_model {str(init_model)} OK\n')
        for ii in jtmp['data']:
            assert(Path(ii).exists())
            assert((ii in init_data_str) or (ii in iter_data_str))
            with log.open("a") as f:
                f.write(f'data {str(ii)} OK\n')
        assert(script.exists())
        with log.open("a") as f:
            f.write(f'script {str(script)} OK\n')

        with model.open("w") as f:
            f.write('read from init model: \n')
            f.write(init_model.read_text() + '\n')
        with lcurve.open("w") as f:
            f.write('read from train_script: \n')
            f.write(script.read_text() + '\n')

        os.chdir(cwd)
        
        return OPIO({
            'script' : work_dir/oscript,
            'model' : work_dir/model,
            'lcurve' : work_dir/lcurve,
            'log' : work_dir/log
        })


def _check_log(
        tcase,
        fname, 
        path,
        script,
        init_model,
        init_data,
        iter_data,
        only_check_name = False
):
    with open(fname) as fp:
        lines_ = fp.read().strip().split('\n')    
    if only_check_name:
        lines = []
        for ii in lines_:
            ww = ii.split(' ')
            ww[1] = str(Path(ww[1]).name)
            lines.append(' '.join(ww))
    else:
        lines = lines_
    revised_fname = lambda ff : Path(ff).name if only_check_name else Path(ff)
    tcase.assertEqual(
        lines[0].split(' '),
        ['init_model', str(revised_fname(Path(path)/init_model)), 'OK']
    )
    for ii in range(2):        
        tcase.assertEqual(
            lines[1+ii].split(' '),
            ['data', str(revised_fname(Path(path)/sorted(list(init_data))[ii])), 'OK']
        )
    for ii in range(2):
        tcase.assertEqual(
            lines[3+ii].split(' '),
            ['data', str(revised_fname(Path(path)/sorted(list(iter_data))[ii])), 'OK']
        )
    tcase.assertEqual(
        lines[5].split(' '),
        ['script', str(revised_fname(Path(path)/script)), 'OK']
    )
    

def _check_model(
        tcase,
        fname,
        path,
        model,
):
    with open(fname) as fp:
        flines = fp.read().strip().split('\n')
    with open(Path(path)/model) as fp:
        mlines = fp.read().strip().split('\n')
    tcase.assertEqual(flines[0], "read from init model: ")
    for ii in range(len(mlines)):
        tcase.assertEqual(flines[ii+1], mlines[ii])

def _check_lcurve(
        tcase,
        fname,
        path,
        script,
):
    with open(fname) as fp:
        flines = fp.read().strip().split('\n')
    with open(Path(path)/script) as fp:
        mlines = fp.read().strip().split('\n')
    tcase.assertEqual(flines[0], "read from train_script: ")
    for ii in range(len(mlines)):
        tcase.assertEqual(flines[ii+1], mlines[ii])

def check_run_train_dp_output(
        tcase,
        work_dir, 
        script, 
        init_model,
        init_data,
        iter_data,
        only_check_name = False,
):
    cwd = os.getcwd()
    os.chdir(work_dir)    
    _check_log(tcase, "log", cwd, script, init_model, init_data, iter_data, only_check_name = only_check_name)
    _check_model(tcase, "model.pb", cwd, init_model)
    _check_lcurve(tcase, "lcurve.out", cwd, script)
    os.chdir(cwd)
    

class TestMockedPrepDPTrain(unittest.TestCase):
    def setUp(self):
        self.numb_models = 3
        self.template_script = { 'seed' : 1024, 'data': [] }
        self.expected_subdirs = ['task.0000', 'task.0001', 'task.0002']
        self.expected_train_scripts = [Path('task.0000/input.json'), Path('task.0001/input.json'), Path('task.0002/input.json')]

    def tearDown(self):
        for ii in self.expected_subdirs:
            if Path(ii).exists():
                shutil.rmtree(ii)

    def test(self):
        prep = MockPrepDPTrain()
        ip = OPIO({
            "template_script" : self.template_script,
            "numb_models" : self.numb_models,
        })
        op = prep.execute(ip)
        self.assertEqual(self.expected_train_scripts, op["train_scripts"])
        self.assertEqual(self.expected_subdirs, op["task_subdirs"])
        

class TestMockRunDPTrain(unittest.TestCase):
    def setUp(self):
        self.numb_models = 3

        tmp_models = []
        for ii in range(self.numb_models):
            ff = Path(f'model_{ii}.pb')
            ff.write_text(f'This is model {ii}')
            tmp_models.append(ff)
        self.init_models = tmp_models
        
        tmp_init_data = [Path('init_data/foo'), Path('init_data/bar')]
        for ii in tmp_init_data:
            ii.mkdir(exist_ok=True, parents=True)
            (ii/'a').write_text('data a')
            (ii/'b').write_text('data b')
        self.init_data = set(tmp_init_data)

        tmp_iter_data = [Path('iter_data/foo'), Path('iter_data/bar')]
        for ii in tmp_iter_data:
            ii.mkdir(exist_ok=True, parents=True)
            (ii/'a').write_text('data a')
            (ii/'b').write_text('data b')
        self.iter_data = set(tmp_iter_data)

        self.template_script = { 'seed' : 1024, 'data': [] }

        self.task_subdirs = ['task.0000', 'task.0001', 'task.0002']
        self.train_scripts = [Path('task.0000/input.json'), Path('task.0001/input.json'), Path('task.0002/input.json')]
        
        for ii in range(3):
            Path(self.task_subdirs[ii]).mkdir(exist_ok=True, parents=True)
            Path(self.train_scripts[ii]).write_text('{}')


    def tearDown(self):
        for ii in ['init_data', 'iter_data' ] + self.task_subdirs:
            if Path(ii).exists():
                shutil.rmtree(str(ii))
        for ii in self.init_models:
            if Path(ii).exists():
                os.remove(ii)

    def test(self):
        for ii in range(3):
            run = MockRunDPTrain()
            ip = OPIO({
                "task_subdir": self.task_subdirs[ii],
                "train_script": self.train_scripts[ii],
                "init_model" : self.init_models[ii],
                "init_data" : self.init_data,
                "iter_data" : self.iter_data,            
            })
            op = run.execute(ip)
            self.assertEqual(op["script"], Path(f'task.{ii:04d}/input.json'))
            self.assertTrue(op["script"].is_file())
            self.assertEqual(op["model"], Path(f'task.{ii:04d}/model.pb'))
            self.assertEqual(op["log"], Path(f'task.{ii:04d}/log'))
            self.assertEqual(op["lcurve"], Path(f'task.{ii:04d}/lcurve.out'))
            check_run_train_dp_output(
                self, 
                self.task_subdirs[ii], 
                self.train_scripts[ii], 
                self.init_models[ii], 
                self.init_data, 
                self.iter_data
            )


class TestTrainDp(unittest.TestCase):
    def setUp (self) :
        self.numb_models = 3

        tmp_models = []
        for ii in range(self.numb_models):
            ff = Path(f'model_{ii}.pb')
            ff.write_text(f'This is model {ii}')
            tmp_models.append(ff)
        self.init_models = upload_artifact(tmp_models)
        self.str_init_models = tmp_models
        
        tmp_init_data = [Path('init_data/foo'), Path('init_data/bar')]
        for ii in tmp_init_data:
            ii.mkdir(exist_ok=True, parents=True)
            (ii/'a').write_text('data a')
            (ii/'b').write_text('data b')
        self.init_data = upload_artifact(tmp_init_data)
        self.path_init_data = set(tmp_init_data)

        tmp_iter_data = [Path('iter_data/foo'), Path('iter_data/bar')]
        for ii in tmp_iter_data:
            ii.mkdir(exist_ok=True, parents=True)
            (ii/'a').write_text('data a')
            (ii/'b').write_text('data b')
        self.iter_data = upload_artifact(tmp_iter_data)
        self.path_iter_data = set(tmp_iter_data)

        self.template_script = { 'seed' : 1024, 'data': [] }

        self.task_subdirs = ['task.0000', 'task.0001', 'task.0002']
        self.train_scripts = [
            Path('task.0000/input.json'), 
            Path('task.0001/input.json'), 
            Path('task.0002/input.json'),
        ]


    def tearDown(self):
        for ii in ['init_data', 'iter_data' ] + self.task_subdirs:
            if Path(ii).exists():
                shutil.rmtree(str(ii))
        for ii in self.str_init_models:
            if Path(ii).exists():
                os.remove(ii)


    def test_train(self):
        steps = steps_train(
            "train-steps",
            MockPrepDPTrain,
            MockRunDPTrain,
        )
        train_step = Step(
            'train-step', 
            template = steps,
            parameters = {
                "numb_models" : self.numb_models,
                "template_script" : self.template_script,
            },
            artifacts = {
                "init_models" : self.init_models,
                "init_data" : self.init_data,
                "iter_data" : self.iter_data,
            },
        )
        wf = Workflow(name="dp-train")
        wf.add(train_step)
        wf.submit()
        
        while wf.query_status() in ["Pending", "Running"]:
            time.sleep(4)

        self.assertEqual(wf.query_status(), "Succeeded")
        step = wf.query_step(name="train-step")[0]
        self.assertEqual(step.phase, "Succeeded")

        download_artifact(step.outputs.artifacts["scripts"])
        download_artifact(step.outputs.artifacts["models"])
        download_artifact(step.outputs.artifacts["logs"])
        download_artifact(step.outputs.artifacts["lcurves"])

        for ii in range(3):
            check_run_train_dp_output(
                self, 
                self.task_subdirs[ii], 
                self.train_scripts[ii], 
                self.str_init_models[ii], 
                self.path_init_data, 
                self.path_iter_data,
                only_check_name = True
            )

        
