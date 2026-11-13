/** Every internal link and asset goes through `href` so the `/durableq/` base path is never forgotten. */
const base = import.meta.env.BASE_URL.replace(/\/$/, '');

export const href = (path = ''): string => `${base}/${path.replace(/^\//, '')}`;

export const REPO = 'https://github.com/Eliot-2006/durableq';

/** Link to a file (optionally `path#anchor`) on the default branch. */
export const blob = (path: string): string => `${REPO}/blob/main/${path}`;
