import os
import pandas as pd
import numpy as np


def get_data(path, date):
    df = pd.read_csv(os.path.join(path, f'{date}.csv'), index_col=0)
    df.index = df.index.astype('str').str.zfill(6)
    df = df[(df.index >= '093000') & (df.index <= '145700')]
    return df

def score(dates, std_path, eval_path):
    for i in range(1, 21):
        err_list = []
        for date in dates:
            standard_df, test_df = get_data(std_path, date), get_data(eval_path, date)
            err_list.append(np.average(abs(standard_df[f'alpha_{i}'] - test_df[f'alpha_{i}']) / abs(standard_df[f'alpha_{i}'] + 1e-7)))

        print(f'alpha_{i}:', True if (np.average(err_list) < 0.01) else False)


if __name__ == "__main__":
    dates = ['0102', '0103', '0104', '0105', '0108']
    std_path = ''   # 标准答案路径
    eval_path = ''   # 你的答案路径
    score(dates=dates, std_path=std_path, eval_path=eval_path)