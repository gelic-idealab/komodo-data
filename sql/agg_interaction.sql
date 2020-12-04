SELECT
    i.capture_id,
    c.start as capture_start,
    i.session_id,
    client_id,
    source_id,
    target_id, 
    it.type,
    count(*) as count
FROM komodo.interaction i
JOIN komodo.interaction_type it ON i.interaction_type = it.id
JOIN komodo.capture c ON i.capture_id = c.capture_id
GROUP BY capture_id, c.start, session_id, client_id, source_id, target_id, it.type
ORDER BY capture_id, c.start, session_id, client_id, source_id, target_id, it.type;