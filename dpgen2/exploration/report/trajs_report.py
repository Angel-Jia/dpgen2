import numpy as np
import random
from . import ExplorationReport
from typing import (
    List,
    Tuple,
    Union
)
from dflow.python import FatalError

class TrajsExplorationReport(ExplorationReport):
    def __init__(
            self,
    ):
        self.clear()

    def clear(
            self,
    ):
        self.traj_nframes = []
        self.traj_cand = []
        self.traj_accu = []
        self.traj_fail = []        
        self.traj_cand_picked = []

    def record_traj(
            self,
            id_f_accu,
            id_f_cand,
            id_f_fail,
            id_v_accu,
            id_v_cand,
            id_v_fail,
    ):
        """
        Record one trajctory. inputs are the indexes of candidate, accurate and failed frames. 

        """
        # check consistency
        novirial = id_v_cand is None
        if novirial:
            assert(id_v_accu is None)
            assert(id_v_fail is None)
        nframes = np.size(np.concatenate((id_f_cand, id_f_accu, id_f_fail)))
        if (not novirial) and nframes != np.size(np.concatenate((id_v_cand, id_v_accu, id_v_fail))):
            raise FatalError("number of frames by virial ")
        # nframes
        # to sets
        set_f_accu = set(id_f_accu)
        set_f_cand = set(id_f_cand)
        set_f_fail = set(id_f_fail)
        set_v_accu = set([ii for ii in range(nframes)]) if novirial else set(id_v_accu)
        set_v_cand = set([]) if novirial else set(id_v_cand)
        set_v_fail = set([]) if novirial else set(id_v_fail)
        # accu, cand, fail
        set_accu = set_f_accu & set_v_accu
        set_cand = ( (set_f_cand & set_v_accu) | 
                     (set_f_cand & set_v_cand) | 
                     (set_f_accu & set_v_cand) )
        set_fail = ( set_f_fail | set_v_fail)
        # check size
        assert(nframes == len(set_accu | set_cand | set_fail))
        assert(0 == len(set_accu & set_cand))
        assert(0 == len(set_accu & set_fail))
        assert(0 == len(set_cand & set_fail))
        # record
        self.traj_nframes.append(nframes)
        self.traj_cand.append(set_cand)
        self.traj_accu.append(set_accu)
        self.traj_fail.append(set_fail)

        
    def failed_ratio(
            self,
            tag = None,
    ):
        traj_nf = [len(ii) for ii in self.traj_fail]
        return float(sum(traj_nf)) / float(sum(self.traj_nframes))

    def accurate_ratio(
            self,
            tag = None,
    ):
        traj_nf = [len(ii) for ii in self.traj_accu]
        return float(sum(traj_nf)) / float(sum(self.traj_nframes))

    def candidate_ratio(
            self,
            tag = None,
    ):
        traj_nf = [len(ii) for ii in self.traj_cand]
        return float(sum(traj_nf)) / float(sum(self.traj_nframes))

    def get_candidates(
            self,
            max_nframes : int = None,
    )->List[Tuple[int,int]]:
        """
        Get candidates. If number of candidates is larger than `max_nframes`, 
        then randomly pick `max_nframes` frames from the candidates. 

        Parameters
        ----------
        max_nframes    int
                The maximal number of frames of candidates.

        Returns
        -------
        cand_frames   List[Tuple[int,int]]
                Candidate frames. A list of tuples: [(traj_idx, frame_idx), ...]
        """
        self.traj_cand_picked = []
        for tidx,tt in enumerate(self.traj_cand):
            for ff in tt:
                self.traj_cand_picked.append((tidx, ff))
        if max_nframes and max_nframes < len(self.traj_cand_picked):
            random.shuffle(self.traj_cand_picked)
            ret = sorted(self.traj_cand_picked[:max_nframes])
        else:
            ret = self.traj_cand_picked
        return ret
        

class TrajsDistExplorationReport(ExplorationReport):
    def __init__(
            self,
    ):
        self.clear()

    def clear(
            self,
    ):
        self.traj_nframes = []
        self.traj_cand = []
        self.traj_accu = []
        self.traj_fail = []        
        self.traj_cand_picked = []

    def record_traj(
            self,
            id_f_accu: Union[np.ndarray, List],
            id_f_cand: Union[np.ndarray, List],
            id_f_fail: Union[np.ndarray, List],
            id_f_cand_value: Union[np.ndarray, List]
    ):
        """
        Record one trajctory. inputs are the indexes of candidate, accurate and failed frames. 

        """
        # check consistency
        nframes = np.size(np.concatenate((id_f_cand, id_f_accu, id_f_fail)))

        # nframes
        # to sets
        set_f_accu = set(id_f_accu)
        set_f_cand = set(id_f_cand)
        id_f_cand_value_dict = {int(_id): float(value)
                                for _id, value in zip(id_f_cand, id_f_cand_value)}
        set_f_fail = set(id_f_fail)

        # accu, cand, fail
        set_accu = set_f_accu
        set_cand = set_f_cand
        set_fail = set_f_fail
        # check size
        assert(nframes == len(set_accu | set_cand | set_fail))
        assert(0 == len(set_accu & set_cand))
        assert(0 == len(set_accu & set_fail))
        assert(0 == len(set_cand & set_fail))
        # record
        self.traj_nframes.append(nframes)

        self.traj_cand.append([item for item in id_f_cand_value_dict.items() if item[0] in set_cand])
        
        self.traj_accu.append(set_accu)
        self.traj_fail.append(set_fail)

        
    def failed_ratio(
            self,
            tag = None,
    ):
        traj_nf = [len(ii) for ii in self.traj_fail]
        return float(sum(traj_nf)) / float(sum(self.traj_nframes))

    def accurate_ratio(
            self,
            tag = None,
    ):
        traj_nf = [len(ii) for ii in self.traj_accu]
        return float(sum(traj_nf)) / float(sum(self.traj_nframes))

    def candidate_ratio(
            self,
            tag = None,
    ):
        traj_nf = [len(ii) for ii in self.traj_cand]
        return float(sum(traj_nf)) / float(sum(self.traj_nframes))

    def get_candidates(
            self,
            max_nframes : Union[int, float] = None,
    )->List[Tuple[int,int]]:
        """
        Get candidates. If number of candidates is larger than `max_nframes`, 
        then randomly pick `max_nframes` frames from the candidates. 

        Parameters
        ----------
        max_nframes    int
                The maximal number of frames of candidates.

        Returns
        -------
        cand_frames   List[Tuple[int,int,max_devi_f]]
                Candidate frames. A list of tuples: [(traj_idx, frame_idx, max_devi_f), ...]
        """
        self.traj_cand_picked = []
        for tidx,tt in enumerate(self.traj_cand):
            for ff in tt:
                self.traj_cand_picked.append((tidx, *ff))
        if isinstance(max_nframes, int) and max_nframes > 0 and max_nframes < len(self.traj_cand_picked):
            ret = sorted(self.traj_cand_picked, key=lambda x: x[2], reverse=True)[:max_nframes]
        elif isinstance(max_nframes, float) and max_nframes > 0 and max_nframes < 1.0:
            numb = int(len(self.traj_cand_picked) * max_nframes + 0.5)
            numb = max(1, numb)
            ret = sorted(self.traj_cand_picked, key=lambda x: x[2], reverse=True)[:numb]
        else:
            ret = self.traj_cand_picked
        
        total_frames = sum(self.traj_nframes)
        total_cand = sum([len(x) for x in self.traj_cand])
        total_accu = sum([len(x) for x in self.traj_accu])
        total_failed = sum([len(x) for x in self.traj_fail])
        
        print('total frames: {}, id_f_cand: {} ({:.2%}), id_f_accu: {} ({:.2%}), id_f_fail: {} ({:.2%})'.format(
            total_frames, total_cand, total_cand / total_frames,
            total_accu, total_accu / total_frames,
            total_failed, total_failed / total_frames
        ))
        
        hist,bins = np.histogram([v[2] for v in self.traj_cand_picked],bins=20,range=(0.,1), density=True)
        print([round(x, 4) for x in (hist * (bins[1] - bins[0])).tolist()])
        print(bins.tolist())

        max_devi_f_list = np.array([item[2] for item in ret])
        print('max_devi_f mean:', np.mean(max_devi_f_list))
        print('max_devi_f median:', np.median(max_devi_f_list))
        print('max_devi_f max:', max_devi_f_list[0])
        print('max_devi_f min:', max_devi_f_list[-1])
        return ret