import itertools
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pprint import pprint
from typing import Union, Callable
import pandas as pd


@dataclass
class BenchParameter:
    name: str
    items: Union[list, dict]


@dataclass
class CallResult:
    parameters: dict
    start_time: datetime
    runtime: timedelta
    result_content: dict

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
    parameters: dict
    func: Callable
    results: list[CallResult] = field(default_factory=list)

    def run(self):
        results = []
        for pc in self.parameter_combinations():
            start_time = datetime.now()
            result = self.func(**pc)
            runtime = datetime.now() - start_time
            cr = CallResult(
                parameters=pc,
                result_content=result,
                start_time=start_time,
                runtime=runtime
            )
            self.results.append(cr)

    def results_as_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame([r.as_dict() for r in self.results])

    def parameter_combinations(self):
        p_keys, p_values = zip(*self.parameters.items())
        return [dict(zip(p_keys, c)) for c in itertools.product(*p_values)]


if __name__ == "__main__":
    def make_request(file, access_point):
        print(file)
        print(f"downloading {file} from {access_point}")
        return {"status": 200}


    files = ["payment_10.csv", "payment_100.csv"]
    access_points = ["gcp", "aws"]
    bench = Bench(
        parameters={"file": files, "access_point": access_points},
        func=make_request
    )

    bench.run()
    df = bench.results_as_dataframe()
    pprint(df.head())
    pprint(df.columns)
