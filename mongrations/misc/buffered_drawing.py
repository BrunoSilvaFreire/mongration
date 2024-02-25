from cmath import sqrt
from math import copysign


class BufferedCanvas:
    def __init__(self):
        self.buffer = [[" "]]

    def _ensure_capacity(self, new_x, new_y):
        current_height = len(self.buffer)
        if new_y >= current_height:
            for _ in range(new_y - current_height + 1):
                self.buffer.append([" "] * len(self.buffer[0]))

        current_width = len(self.buffer[0])
        if new_x >= current_width:
            for row in self.buffer:
                row.extend([" "] * (new_x - current_width + 1))

    def draw_circle(self, center: (int, int), radius: int):
        center_x, center_y = center
        line_threshold = 0.5
        for x in range(center_x - radius, center_x + radius + 1):
            for y in range(center_y - radius, center_y + radius + 1):
                distance_to_center = sqrt((x - center_x) ** 2 + (y - center_y) ** 2)
                if abs(radius - distance_to_center) <= line_threshold:
                    char = '#'
                else:
                    char = ' '

                self[x, y] = char

    def __setitem__(self, key, value):
        x, y = key
        self._ensure_capacity(x, y)
        self.buffer[y][x] = value

    def __str__(self):
        return "\n".join("".join(row) for row in self.buffer)

    def draw_centered(self, center: (int, int), text: str, height: int = None, width: int = None):
        center_x, center_y = center

        # If height and width are not specified, default to text dimensions
        if height is None or width is None:
            text_height = 1  # Since text is one line
            text_width = len(text)
        else:
            text_height = height
            text_width = width

        # Calculate start positions to center the text within the box
        start_x = center_x - text_width // 2
        start_y = center_y - text_height // 2

        # Calculate the text start position within the centered box
        text_start_x = max(start_x, start_x + (text_width - len(text)) // 2)

        # Ensure the canvas can accommodate the box
        self._ensure_capacity(start_x + text_width - 1, start_y + text_height - 1)

        # Clear the area if you want the text to be on a blank space (optional)

        # Draw the text centered within the box
        space_left = text_width
        line = 0
        x = 0
        for char in text:
            self[text_start_x + x, start_y + line] = char
            space_left -= 1
            x += 1
            if space_left == 0:
                line += 1
                x = 0
                space_left = text_width


    def draw_line(self, start: (int, int), end: (int, int), line_width: int = 1):
        start_x, start_y = start
        end_x, end_y = end
        delta_x = abs(end_x - start_x)
        delta_y = abs(end_y - start_y)
        sign_x = int(copysign(1, end_x - start_x))
        sign_y = int(copysign(1, end_y - start_y))
        error = delta_x - delta_y

        while True:
            self._draw_line_segment(start_x, start_y, line_width)
            if start_x == end_x and start_y == end_y:
                break
            error2 = 2 * error
            if error2 > -delta_y:
                error -= delta_y
                start_x += sign_x
            if error2 < delta_x:
                error += delta_x
                start_y += sign_y

    def _draw_line_segment(self, x, y, width):
        # Draw a segment of the line with the specified width
        offset = width // 2
        for line_offset in range(-offset, offset + width % 2):
            self[x, y + line_offset] = "*"
