import numpy as np
import matplotlib.pyplot as plt


def visualize_colormaps(colormap_names):
    nrows = len(colormap_names)
    gradient = np.linspace(0, 1, 256)
    gradient = np.vstack((gradient, gradient))

    fig, axes = plt.subplots(nrows=nrows, figsize=(6, 2 * nrows))
    for ax, name in zip(axes, colormap_names):
        ax.imshow(gradient, aspect='auto', cmap=plt.get_cmap(name=name))
        ax.set_title(name, fontsize=14)
        ax.axis('off')

    plt.tight_layout()
    plt.show()

if __name__ == '__main__':

    # List of colormap names to visualize
    colormap_names = ['viridis', 'plasma', 'inferno', 'magma', 'cividis']

    visualize_colormaps(colormap_names)
