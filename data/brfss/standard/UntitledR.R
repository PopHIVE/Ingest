# cat_with_rainbow.R
# Draw a cat with a rainbow over its head using base R graphics

draw_arc <- function(cx, cy, r, start = 0, end = pi, n = 200, col = "red", lwd = 10) {
  theta <- seq(start, end, length.out = n)
  x <- cx + r * cos(theta)
  y <- cy + r * sin(theta)
  lines(x, y, lwd = lwd, col = col, lend = "round")
}

open_canvas <- function(width = 800, height = 800, file = NULL) {
  if (!is.null(file)) {
    png(filename = file, width = width, height = height, res = 150)
  } else {
    dev.new(width = width/96, height = height/96) # approximate inches for screen
  }
  par(mar = c(0,0,0,0))
  plot(NA, xlim = c(0, 10), ylim = c(0, 10), asp = 1, xaxt = "n", yaxt = "n", xlab = "", ylab = "", bty = "n")
}

close_canvas <- function(file = NULL) {
  if (!is.null(file)) dev.off()
}

# Main drawing function
draw_cat_with_rainbow <- function(save_file = NULL) {
  open_canvas(file = save_file)
  
  # Background sky
  rect(0, 0, 10, 10, col = "#E6F7FF", border = NA)
  
  # Rainbow (ordered outer -> inner)
  rainbow_colors <- c("#E50000", "#FF8C00", "#FFD300", "#28A92B", "#0077FF", "#6F2DA8")
  
  cx <- 5      # center x
  cy <- 5.8    # slightly above the cat’s head
  base_r <- 3.0
  lwds <- seq(32, 8, length.out = length(rainbow_colors))
  
  # note: start=0, end=pi draws the rainbow arching upward
  for (i in seq_along(rainbow_colors)) {
    draw_arc(cx = cx, cy = cy, r = base_r - (i-1)*0.35,
             start = 0, end = pi, n = 500, col = rainbow_colors[i], lwd = lwds[i])
  }
  
  # A soft cloud under the rainbow
  cloud <- function(cx, cy, scale = 1.0) {
    xs <- c(cx - 1.2, cx - 0.5, cx + 0.6, cx + 1.8, cx + 1.2, cx - 0.4, cx - 1.0)
    ys <- c(cy - 0.2, cy + 0.6, cy + 0.8, cy + 0.2, cy - 0.6, cy - 0.7, cy - 0.1)
    polygon(xs*scale + 0, ys*scale + 0, col = "white", border = NA)
    # fluffy circles
    symbols(x = c(cx-0.7, cx, cx+0.7, cx+1.2), y = c(cy, cy+0.2, cy+0.15, cy-0.2),
            circles = c(0.5, 0.55, 0.45, 0.3)*scale, add = TRUE, inches = FALSE, bg = "white", fg = NA)
  }
  #cloud(cx = 5, cy = 6.9, scale = 1.2)
  
  # Cat body (ellipse)
  draw_ellipse <- function(cx, cy, a, b, col = "black", border = "black", lwd = 1) {
    ang <- seq(0, 2*pi, length.out = 200)
    x <- cx + a * cos(ang)
    y <- cy + b * sin(ang)
    polygon(x, y, col = col, border = border, lwd = lwd)
  }
  draw_ellipse(5, 3.8, 0.9, 0.7, col = "black", border = "#6B4A38", lwd = 2)
  
  # Cat head
  symbols(5, 5, circles = 0.9, inches = FALSE, add = TRUE, bg = "black", fg = "#6B4A38", lwd = 2)
  
  # Ears (triangles)
  polygon(c(4.1, 4.4, 4.6), c(5.6, 6.3, 5.9), col = "black", border = "#6B4A38", lwd = 2)
  polygon(c(5.9, 5.6, 5.4), c(5.6, 6.3, 5.9), col = "black", border = "#6B4A38", lwd = 2)
  # inner ears
  polygon(c(4.3, 4.5, 4.55), c(5.8, 6.1, 5.95), col = "#FFC9C9", border = NA)
  polygon(c(5.7, 5.5, 5.45), c(5.8, 6.1, 5.95), col = "#FFC9C9", border = NA)
  
  # Eyes
  points(x = c(4.55, 5.45), y = c(4.9, 4.9), pch = 21, bg = "white", cex = 2.5, lwd = 2)
  points(x = c(4.55, 5.45), y = c(4.95, 4.95), pch = 19, col = "#1A3B1A", cex = 0.9)
  points(x = c(4.7, 5.3), y = c(4.95, 4.95), pch = 21, bg = "#88D27A", cex = 0.35, col = NA) # sparkle
  
  # Nose
  polygon(c(4.9, 5.1, 5.0), c(4.5, 4.5, 4.35), col = "#FF8DAA", border = "#6B4A38", lwd = 1)
  
  # Mouth
  lines(x = c(5.0, 5.0), y = c(4.35, 4.15), lwd = 2, col = "#6B4A38")
  lines(x = c(5.0, 4.82), y = c(4.15, 4.02), lwd = 2, col = "#6B4A38")
  lines(x = c(5.0, 5.18), y = c(4.15, 4.02), lwd = 2, col = "#6B4A38")
  
  # Whiskers
  whisker <- function(x0, y0, dir = 1) {
    segments(x0, y0, x0 + 1.3*dir, y0 + 0.05, lwd = 2, col = "#6B4A38")
    segments(x0, y0 - 0.12, x0 + 1.1*dir, y0 - 0.18, lwd = 2, col = "#6B4A38")
    segments(x0, y0 + 0.12, x0 + 1.1*dir, y0 + 0.28, lwd = 2, col = "#6B4A38")
  }
  whisker(4.6, 4.4, dir = -1)
  whisker(5.4, 4.4, dir = 1)
  
  # Front paws - moved upward and slightly inward to connect to body
  draw_ellipse(4.3, 2.9, 0.3, 0.4, col = "black", border = "#6B4A38", lwd = 1.5)
  draw_ellipse(5.7, 2.9, 0.3, 0.4, col = "black", border = "#6B4A38", lwd = 1.5)
  
  # Add connecting polygons so paws appear attached
polygon(c(4.0, 4.6, 4.6, 4.0),
        c(3.2, 3.2, 3.8, 3.8),
        col = "black", border = NA)
polygon(c(5.4, 6.0, 6.0, 5.4),
        c(3.2, 3.2, 3.8, 3.8),
        col = "black", border = NA)
  
  # Tail
  tail_x <- c(6.6, 7.2, 7.6, 7.9)
  tail_y <- c(3.6, 3.9, 4.6, 5.2)
  lines(tail_x, tail_y, lwd = 14, col = "black", lend = "round")
  lines(tail_x, tail_y, lwd = 2, col = "#6B4A38", lend = "round")
  
  # Add second gray-striped cat
  # ----------------------------------------
  # Same draw_ellipse() function is already defined above, so reuse it.
  
  # Body (slightly skinnier and lighter)
  draw_ellipse(7.3, 3.8, 0.7, 0.65, col = "gray70", border = "gray40", lwd = 2)
  
  # Head
  symbols(7.3, 5, circles = 0.75, inches = FALSE, add = TRUE, bg = "gray70", fg = "gray40", lwd = 2)
  
  # Ears
  polygon(c(6.6, 6.9, 7.0), c(5.5, 6.0, 5.7), col = "gray70", border = "gray40", lwd = 2)
  polygon(c(8.0, 7.7, 7.6), c(5.5, 6.0, 5.7), col = "gray70", border = "gray40", lwd = 2)
  # inner ears
  polygon(c(6.8, 6.9, 7.0), c(5.75, 5.95, 5.85), col = "#FFC9C9", border = NA)
  polygon(c(7.8, 7.7, 7.6), c(5.75, 5.95, 5.85), col = "#FFC9C9", border = NA)
  
  # Eyes
  points(x = c(6.95, 7.65), y = c(4.9, 4.9), pch = 21, bg = "white", cex = 2.3, lwd = 2)
  points(x = c(6.95, 7.65), y = c(4.95, 4.95), pch = 19, col = "#1A3B1A", cex = 0.9)
  points(x = c(7.1, 7.5), y = c(4.95, 4.95), pch = 21, bg = "#88D27A", cex = 0.35, col = NA)
  
  # Nose and mouth
  polygon(c(7.15, 7.25, 7.2), c(4.5, 4.5, 4.38), col = "#FF8DAA", border = "gray40", lwd = 1)
  lines(x = c(7.2, 7.2), y = c(4.38, 4.2), lwd = 2, col = "gray40")
  lines(x = c(7.2, 7.05), y = c(4.2, 4.05), lwd = 2, col = "gray40")
  lines(x = c(7.2, 7.35), y = c(4.2, 4.05), lwd = 2, col = "gray40")
  
  # Whiskers
  segments(6.8, 4.4, 5.7, 4.45, lwd = 2, col = "gray40")
  segments(6.8, 4.3, 5.75, 4.15, lwd = 2, col = "gray40")
  segments(6.8, 4.5, 5.75, 4.65, lwd = 2, col = "gray40")
  segments(7.6, 4.4, 8.7, 4.45, lwd = 2, col = "gray40")
  segments(7.6, 4.3, 8.65, 4.15, lwd = 2, col = "gray40")
  segments(7.6, 4.5, 8.65, 4.65, lwd = 2, col = "gray40")
  
  # Paws (connected)
  draw_ellipse(6.9, 2.9, 0.25, 0.35, col = "gray70", border = "gray40", lwd = 1.5)
  draw_ellipse(7.7, 2.9, 0.25, 0.35, col = "gray70", border = "gray40", lwd = 1.5)
  polygon(c(6.8, 7.0, 7.0, 6.8), c(3.2, 3.2, 3.7, 3.7), col = "gray70", border = NA)
  polygon(c(7.6, 7.8, 7.8, 7.6), c(3.2, 3.2, 3.7, 3.7), col = "gray70", border = NA)
  
  # Tail with stripes
  tail_x2 <- c(8.0, 8.5, 8.9, 9.1)
  tail_y2 <- c(3.8, 4.2, 4.8, 5.1)
  lines(tail_x2, tail_y2, lwd = 12, col = "gray70", lend = "round")
  lines(tail_x2, tail_y2, lwd = 2, col = "gray40", lend = "round")
  
  # Stripes on back
  for (sx in seq(6.7, 7.9, by = 0.3)) {
    segments(sx, 4.0, sx + 0.2, 4.6, lwd = 2, col = "black")
  }
  # Stripes on tail
  for (i in seq(0, 1, by = 0.25)) {
    lines(tail_x2 + i*0.05, tail_y2 + i*0.05, lwd = 3, col = "black")
  }
  
  # Tiny heart on chest
  polygon(c(4.95, 5.0, 5.05), c(3.6, 3.78, 3.6), col = "#FF6B88", border = NA)
  symbols(4.92, 3.75, circles = 0.03, add = TRUE, inches = FALSE, bg = "#FF6B88", fg = NA)
  symbols(5.08, 3.75, circles = 0.03, add = TRUE, inches = FALSE, bg = "#FF6B88", fg = NA)
  
  # Optional text
  text(5, 0.7, labels = "Happy Rainbow Cat", cex = 1.2, col = "#444444", font = 2)
  
  close_canvas(file = save_file)
}

# Run and show on screen:
draw_cat_with_rainbow(save_file = NULL)

# To save to a PNG file, call:
# draw_cat_with_rainbow(save_file = "cat_rainbow.png")

