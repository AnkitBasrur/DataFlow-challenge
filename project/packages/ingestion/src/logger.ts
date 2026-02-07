export const logger = {
  info: (obj: any, msg?: string) => console.log(msg ?? "", obj ?? ""),
  warn: (obj: any, msg?: string) => console.warn(msg ?? "", obj ?? ""),
  error: (obj: any, msg?: string) => console.error(msg ?? "", obj ?? ""),
};
