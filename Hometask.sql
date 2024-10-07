with filtered_channels as (
    select * 
    from youtube_channels
    where channel_subs >= 1000
),
filtered_videos as (
    select vs.*
    from videos_stat vs
    join videos v ON vs.id = v.video_id
    join filtered_channels fc ON v.channel_id = fc.channel_id
    where views >= 100
),
min_max_values as (
    select 
        fv.id,
        fv.views,
        fv.likes,
        fv.comments,
        v.channel_id,
        min(fv.views) over (partition by v.channel_id) as min_views,
        max(fv.views) over (partition by v.channel_id) as max_views,
        min(fv.likes) over (partition by v.channel_id) as min_likes,
        max(fv.likes) over (partition by v.channel_id) as max_likes,
        min(fv.comments) over (partition by v.channel_id) as min_comments,
        max(fv.comments) over (partition by v.channel_id) as max_comments
    from filtered_videos fv
    join videos v on fv.id = v.video_id
),
normalization_moment as (
    select 
        id,
        case when max_views = min_views then 1 else 1 + 9 * (views - min_views) / (max_views - min_views) end as defined_views,
        case when max_likes = min_likes then 1 else 1 + 9 * (likes - min_likes) / (max_likes - min_likes) end as defined_likes,
        case when max_comments = min_comments then 1 else 1 + 9 * (comments - min_comments) / (max_comments - min_comments) end as defined_comments
    from min_max_values
),
final_scores as (
    select 
        id,
        (0.35 * defined_views + 0.25 * defined_likes + 0.4 * defined_comments + + RAND() * 5) as score
    from normalization_moment
)
select id, score
from final_scores
order by score desc
limit 10;







