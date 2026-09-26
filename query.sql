-- name: RegisterUser :execrows
INSERT INTO users (blog_platform, user_id)
VALUES ($1, $2)
ON CONFLICT (blog_platform, user_id) DO NOTHING;
