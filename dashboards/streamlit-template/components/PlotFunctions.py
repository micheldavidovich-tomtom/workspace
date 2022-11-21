def example_plot(df, letter = "A"):

    ax = df[letter].plot()

    return ax.get_figure()