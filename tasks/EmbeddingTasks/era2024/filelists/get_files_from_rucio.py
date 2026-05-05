import json
import subprocess
import argparse
from tqdm import tqdm


def run_command(cmd):
    results = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if results.returncode != 0:
        raise RuntimeError(f"Command failed: {results.stderr}")
    return results.stdout.strip().split("\n")


def rucio_list_datasets(did):
    return run_command(f"rucio did list --filter 'type==DATASET' --short {did}*")


def rucio_list_files(did):
    return run_command(f"rucio did content list --short {did}")


if __name__ == "__main__":
    args = argparse.ArgumentParser(description="Get files from Rucio")
    args.add_argument("--dataset", required=True, help="Rucio dataset name")
    args.add_argument("--output", required=True, help="Output JSON file")
    args = args.parse_args()

    datasets = rucio_list_datasets(args.dataset)
    files = {}
    for dataset in tqdm(datasets):
        files[dataset] = rucio_list_files(dataset)

    with open(args.output, "w") as f:
        json.dump(files, f, indent=4)

    # # divide datasets into 3 batches and save the filelist to txt files
    # batch_size = len(files) // 3
    # batch_0 = datasets[:batch_size]
    # batch_1 = datasets[batch_size : 2 * batch_size]
    # batch_2 = datasets[2 * batch_size :]
    # for i, b in enumerate([batch_0, batch_1, batch_2]):
    #     with open(f"batch_{i}.filelist", "w") as f:
    #         for dataset in b:
    #             for file in files[dataset]:
    #                 f.write(file + "\n")
    #     with open(f"batch_{i}_datasets.txt", "w") as f:
    #         for dataset in b:
    #             f.write(dataset + "\n")

    # # validate that the number of files in the txt files matches the number of files in the JSON file
    # total_files = sum(len(files[dataset]) for dataset in datasets)
    # txt_files = []
    # for i, b in enumerate([batch_0, batch_1, batch_2]):
    #     with open(f"batch_{i}.filelist", "r") as f:
    #         txt_files.extend(f.read().splitlines())
    # assert len(txt_files) == total_files, f"Number of files in txt files ({len(txt_files)}) does not match number of files in JSON file ({total_files})"
    # print(f"Successfully validated that the number of files in the txt files matches the number of files in the JSON file: {total_files} files")

    # create a file with the list of datasets and a file with the list of files
    with open("datasets.txt", "w") as f:
        f.write("\n".join(datasets))
    with open("files.txt", "w") as f:
        for dataset in datasets:
            for file in files[dataset]:
                f.write(file + "\n")
