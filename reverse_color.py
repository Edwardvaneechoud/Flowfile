from PIL import Image
import numpy as np


def invert_png_colors(input_path, output_path):
    """
    Inverts the colors of a PNG image while preserving transparency.

    Args:
        input_path (str): Path to the input PNG image
        output_path (str): Path where the inverted image will be saved
    """
    # Open the image and convert to RGBA if it isn't already
    img = Image.open(input_path).convert('RGBA')

    # Convert image to numpy array
    img_array = np.array(img)

    # Split the array into color channels and alpha
    rgb = img_array[:, :, :3]
    alpha = img_array[:, :, 3]

    # Invert only the RGB channels (255 - original value)
    inverted_rgb = 255 - rgb

    # Reconstruct the image with original alpha channel
    inverted_array = np.dstack((inverted_rgb, alpha))

    # Convert back to PIL Image and save
    inverted_image = Image.fromarray(inverted_array)
    inverted_image.save(output_path, 'PNG')


if __name__ == "__main__":
    # Example usage
    input_image = "dm.png"
    output_image = "dm_inverted.png"

    try:
        invert_png_colors(input_image, output_image)
        print(f"Successfully inverted image and saved to {output_image}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")