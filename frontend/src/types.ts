export interface TrioUpdate {
  route: string;
  rate: number;
}

export type ServerMsg =
  | { type: "snapshot"; trios: TrioUpdate[] }
  | { type: "update"; trio: TrioUpdate };
