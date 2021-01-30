import argparse
from p_adquisition import m_acquisition as mac
from p_wrangling import m_wrangling as mwr
from p_analysis import m_analysis as man

def argument_parser():
    parser = argparse.ArgumentParser(description = 'Specify inputs')
    parser.add_argument("-p",
                        "--path",
                        help="Specify database path. Please, dont be like Lewis (our trainee) and lose like 234567890 minutes putting it in the right way",
                        required=True, type=str)

    parser.add_argument("-c",
                        "--country",
                        help="Select country. All selected by default",
                        type=str, default='all')


    return parser.parse_args()


def main(arguments):
    print('\n\n\nStarting pipeline!',end='')

    print('\n\n\n...\n\n\n')
    df_project = mac.acquire(arguments.path)

    merged_data = mwr.wrangling(df_project)
    merged_data.to_csv('./data/processed/data_merged.csv')

    data_project_analysed = man.analyse_data(merged_data,arguments.country)
    data_project_analysed.to_csv('./data/results/gender_country_analysis.csv')
    print(data_project_analysed)

    # Bonus 1:
    print('Opinions loading!')
    data_opinions = man.position(df_project)
    data_opinions.to_csv('./data/results/bonus_data_opinions.csv')
    print(data_opinions)

    print('========================= Pipeline done! '
          './data/results =========================')


if __name__ == '__main__':
    main(argument_parser())