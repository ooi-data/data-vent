

def _calc_avail_dict(all_df):
    avail_dict = {}
    for _, row in all_df.iterrows():
        if row['dtindex'] not in avail_dict:
            avail_dict[int(row['dtindex'])] = row['count']
        else:
            avail_dict[int(row['dtindex'])] = (
                avail_dict[row['dtindex']] + row['count']
            )
    return avail_dict


def _fetch_avail_dict(ddf, resolution: str = 'D'):
    final_ddf = ddf.resample(resolution).count().reset_index()
    final_ddf['dtindex'] = final_ddf['dtindex'].apply(
        lambda t: t.to_numpy().astype('datetime64[s]').astype('int64'),
        meta=('dtindex', 'int64'),
    )
    return _calc_avail_dict(final_ddf)
