from PIL import Image
import os


def resize_maximum_quality(input_path, output_path, max_size):
  """
  Resize image with absolute maximum quality settings.
  Preserves colors, transparency, and detail.
  """
  with Image.open(input_path) as img:
    # Convert to RGBA with maximum color depth
    if img.mode != 'RGBA':
      img = img.convert('RGBA')

    width, height = img.size

    # Calculate new dimensions
    if width > max_size or height > max_size:
      if width > height:
        new_width = max_size
        new_height = int(height * (max_size / width))
      else:
        new_height = max_size
        new_width = int(width * (max_size / height))

      # For small icons, we'll use a two-step resize for better quality
      if width > max_size * 2 or height > max_size * 2:
        # First step: reduce to intermediate size
        intermediate_width = new_width * 2
        intermediate_height = new_height * 2
        img = img.resize((intermediate_width, intermediate_height), Image.LANCZOS)

      # Final resize with maximum quality settings
      img = img.resize((new_width, new_height), Image.LANCZOS)

    # Save with maximum possible quality
    img.save(
      output_path,
      'PNG',
      optimize=False,  # No compression
      quality=100,  # Maximum quality
      bits=8,  # 8 bits per channel (maximum for PNG)
    )

    return img.size


def process_folder(input_folder, output_folder, max_size=50):
  """
  Process all images in a folder with maximum quality settings.
  """
  os.makedirs(output_folder, exist_ok=True)

  image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp')

  for filename in os.listdir(input_folder):
    if filename.lower().endswith(image_extensions):
      input_path = os.path.join(input_folder, filename)
      output_path = os.path.join(output_folder, os.path.splitext(filename)[0] + '.png')

      try:
        with Image.open(input_path) as img:
          original_size = (img.size[0], img.size[1])

        new_size = resize_maximum_quality(input_path, output_path, max_size)

        # Get file sizes
        original_kb = os.path.getsize(input_path) / 1024
        new_kb = os.path.getsize(output_path) / 1024

        if original_size != new_size:
          print(f"{filename} ({original_size[0]}x{original_size[1]} → {new_size[0]}x{new_size[1]}): "
                f"{original_kb:.1f}KB → {new_kb:.1f}KB")
        else:
          print(f"{filename} ({original_size[0]}x{original_size[1]}): {original_kb:.1f}KB → {new_kb:.1f}KB")

      except Exception as e:
        print(f"Error processing {filename}: {str(e)}")


# Example usage
input_folder = "/Users/edwardvaneechoud/Flowfile/flowfile_frontend/src/renderer/app/features/designer/assets/icons"
output_folder = "processed_images"
process_folder(input_folder, output_folder, 100)