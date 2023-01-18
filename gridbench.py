import itertools
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pprint import pprint
from typing import Union, Callable
import pandas as pd


# @dataclass
# class BenchParameter:
#     name: str
#     items: Union[list, dict]


@dataclass
class CallResult:
    parameters: dict
    start_time: datetime
    runtime: timedelta
    result_content: dict
    run_name: str = ""

    def as_dict(self):
        d = {
            "start_time": self.start_time,
            "runtime": self.runtime.total_seconds(),
        }

        for k, v in self.parameters.items():
            d[f"param_{k}"] = v

        for k, v in self.result_content.items():
            d[f"result_{k}"] = v

        return d


@dataclass
class Bench:
    parameters: dict[str, Union[list,dict]]
    func: Callable
    results: list[CallResult] = field(default_factory=list)

    def run(self, run_name: str = ""):
        if run_name == "":
            run_name = str(datetime.now)
        results = []
        for pc in self.__parameter_combinations():
            # parameters can be scalar or an (alias, value) tuple
            pc_values = {k: v[1] if isinstance(v, tuple) else v for k,v in pc.items()}
            pc_aliases = {k: v[0] if isinstance(v, tuple) else v for k,v in pc.items()}
            start_time = datetime.now()
            result = self.func(**pc_values)
            runtime = datetime.now() - start_time
            cr = CallResult(
                parameters=pc_aliases,
                result_content=result,
                start_time=start_time,
                runtime=runtime,
                run_name=run_name
            )
            self.results.append(cr)

    def results_as_dataframe(self) -> pd.DataFrame:
        """Return the Results of all runs as a Pandas DataFrame"""
        return pd.DataFrame([r.as_dict() for r in self.results])

    def __parameter_combinations(self) -> list[tuple]:
        """Return a list with tuples of combined parameters.

        Each parameter is represented as a tuple of (alias, value).
        """
        p_keys, p_values = zip(*self.parameters.items())
        p_values = [p.items() if isinstance(p, dict) else p for p in p_values]
        return [dict(zip(p_keys, c)) for c in itertools.product(*p_values)]



if __name__ == "__main__":
    def make_request(file, access_point):
        print(file)
        print(f"downloading {file} from {access_point}")
        return {"status": 200}


    files = ["payment_10.csv", "payment_100.csv"]
    access_points = {
        "gcp": "gcp.de",
        "aws": "aws.com",
    }
    bench = Bench(
        parameters={"file": files, "access_point": access_points},
        func=make_request
    )

    bench.run()
    df = bench.results_as_dataframe()
    pprint(df.head())
    pprint(df.columns)
