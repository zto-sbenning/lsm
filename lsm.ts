import { v4 as uuid } from 'uuid';
import { BehaviorSubject, Observable, OperatorFunction, defer, from, Subscription, EMPTY, merge, timer, throwError, of } from 'rxjs';
import { filter, switchMap, exhaustMap, mergeMap, takeUntil, tap, catchError, take, map } from 'rxjs/operators';

export type Execution<I = any, O = any, C = any> = (input: I, context?: C) => Observable<O>;

export type GenericError = { msg: string, code?: number };

export type InputObject<I = any, C = any> = { id: string, context?: C, input?: I, cancel?: true };
export type OutputObject<O = any, C = any> = { id: string, context?: C, output: O };
export type ErrorObject<C = any> = { id: string, context?: C, error: GenericError };

export const inputFactory = <I = any, C = any>(input: I, context?: C): InputObject<I, C> => ({ input, context, id: uuid() });
export const outputFactory = <I = any, O = any, C = any>(output: O, input: InputObject<I>): OutputObject<O, C> => ({ output, context: input.context, id: input.id });
export const errorFactory = <I = any, C = any>(error: GenericError, input: InputObject<I>): ErrorObject<C> => ({ error, context: input.context, id: input.id });

export const isDefined = (src$: Observable<any>) => src$.pipe(filter(value => value !== undefined));
export const isAnInput = (src$: Observable<InputObject>) => src$.pipe(isDefined, filter<InputObject>(({ cancel }) => !cancel));
export const isACancel = (src$: Observable<InputObject>) => src$.pipe(isDefined, filter<InputObject>(({ cancel }) => cancel));
export const isThisCancel = (idRef: string) => (src$: Observable<InputObject>) => src$.pipe(isACancel, filter<InputObject>(({ input }) => input === idRef));
export const isThisOutput = (idRef: string) => (src$: Observable<OutputObject>) => src$.pipe(isDefined, filter<OutputObject>(({ id }) => id === idRef));
export const isThisError = (idRef: string) => (src$: Observable<ErrorObject>) => src$.pipe(isDefined, filter<ErrorObject>(({ id }) => id === idRef));

export enum CombineStratName {
    SWITCH = '[Combine Strategy] Switch',
    EXHAUST = '[Combine Strategy] Exhaust',
    MERGE = '[Combine Strategy] Merge',
};

export enum ExecutionStatus {
    SUCCESS = '[Execution Status] Success',
    FAILURE = '[Execution Status] Failure',
    ABORT = '[Execution Status] Abort',
};

export class ExecutionResult {
    constructor(
        public readonly status: ExecutionStatus,
        public input: InputObject,
        public data: any,
    ) {}
}
export class SuccessResult extends ExecutionResult {
    constructor(input: InputObject, data: any) {
        super(ExecutionStatus.SUCCESS, input, data);
    }
}
export class FailureResult extends ExecutionResult {
    constructor(input: InputObject, data: any) {
        super(ExecutionStatus.FAILURE, input, data);
    }
}
export class AbortResult extends ExecutionResult {
    constructor(input: InputObject) {
        super(ExecutionStatus.ABORT, input, null);
    }
}

export type OperatorFunctionFactory<A extends Array<any> = Array<any>, T = any, U = any> = (...args: A) => OperatorFunction<T, U>;

export abstract class CombineStrat {
    readonly name: CombineStratName;
    abstract applyStrategy(...args: any[]): OperatorFunctionFactory;
}

export class SwitchCombineStrat extends CombineStrat {
    name: CombineStratName = CombineStratName.SWITCH;
    applyStrategy() {
        return switchMap;
    }
}

export class ExhaustCombineStrat extends CombineStrat {
    name: CombineStratName = CombineStratName.EXHAUST;
    applyStrategy() {
        return exhaustMap;
    }
}

export class MergeCombineStrat extends CombineStrat {
    name: CombineStratName = CombineStratName.MERGE;
    applyStrategy() {
        return mergeMap;
    }
}

export class Task<I = any, O = any, C = any> {

    protected input: BehaviorSubject<InputObject<I, C>> = new BehaviorSubject(undefined);
    protected cancel: BehaviorSubject<InputObject<I, C>> = new BehaviorSubject(undefined);
    protected output: BehaviorSubject<OutputObject<O, C>> = new BehaviorSubject(undefined);
    protected error: BehaviorSubject<ErrorObject<C>> = new BehaviorSubject(undefined);

    input$: Observable<InputObject<I, C>>;
    cancel$: Observable<InputObject<I, C>>;
    output$: Observable<OutputObject<O, C>>;
    error$: Observable<ErrorObject<C>>;

    processSubscription: Subscription;

    constructor(
        protected exec: Execution<I, O, C>,
        protected combineStrat: CombineStrat,
        startProcessing: boolean = true,
    ) {
        this.input$ = this.input.pipe(isAnInput);
        this.cancel$ = this.input.pipe(isACancel);
        this.output$ = this.output.pipe(isDefined);
        this.error$ = this.error.pipe(isDefined);
        if (startProcessing) {
            this.startProcessing();
        }
    }

    protected processInput(input: InputObject<I, C>) {
        const thisCancel$ = this.cancel$.pipe(isThisCancel(input.id));
        return this.exec(input.input, input.context).pipe(
            take(1),
            tap(
                output => this.output.next(outputFactory(output, input)),
                error => this.error.next(errorFactory(error, input)),
            ),
            map(output => outputFactory(output, input)),
            takeUntil(thisCancel$),
        );
    }

    startProcessing() {
        const strategy = this.combineStrat.applyStrategy();
        this.processSubscription = this.input$.pipe(
            strategy((input: InputObject) => this.processInput(input).pipe(
                catchError(() => EMPTY)
            )),
        ).subscribe();
    }

    stopProcessing() {
        if (this.processSubscription && !this.processSubscription.closed) {
            this.processSubscription.unsubscribe();
            this.processSubscription = undefined;
        }
    }

    getJob(input: I, context?: C) {
        const id: string = uuid();
        const run = () => {
            this.input.next({ id, input, context });
        };
        const cancel = () => {
            this.input.next({ id, input: id as any, context, cancel: true });
        };
        const process$ = merge(
            this.cancel$.pipe(isThisCancel(id), take(1)),
            this.output$.pipe(isThisOutput(id), take(1)),
            this.error$.pipe(isThisError(id), take(1)),
        ).pipe(
            take(1),
            map(data => {
                if (data && (data as InputObject).cancel) {
                    return new AbortResult({ id, input, context });
                } else if (data && (data as OutputObject).output !== undefined) {
                    return new SuccessResult({ id, input, context }, (data as OutputObject).output);
                } else if (data && (data as ErrorObject).error !== undefined) {
                    return new FailureResult({ id, input, context }, (data as ErrorObject).error);
                }
            })
        );
        return { run, cancel, process$, id };
    }

}

export class SwitchTask<I = any, O = any, C = any> extends Task<I, O, C> {
    constructor(exec: Execution<I, O, C>) {
        super(exec, new SwitchCombineStrat());
    }
}

export class ExhaustTask<I = any, O = any, C = any> extends Task<I, O, C> {
    constructor(exec: Execution<I, O, C>) {
        super(exec, new ExhaustCombineStrat());
    }
}

export class MergeTask<I = any, O = any, C = any> extends Task<I, O, C> {
    constructor(exec: Execution<I, O, C>) {
        super(exec, new MergeCombineStrat());
    }
}


export function test() {
    const service = (v: number, ctx = {}) => timer(ctx && ctx['timer'] || 1500).pipe(
        switchMap(() => {
            if (ctx && ctx['error']) {
                return throwError(ctx['error']);
            } else {
                return of(v * 2);
            }
        })
    );
    const task = new MergeTask(service);

    const job1 = task.getJob(21);
    job1.process$.subscribe(
        res => console.log(`job1 next: `, res),
        err => console.log(`job1 error: `, err),
        () => console.log(`job1 complete.`)
    );   
    job1.run();

    const job2 = task.getJob(84);
    job2.process$.subscribe(
        res => console.log(`job2 next: `, res),
        err => console.log(`job2 error: `, err),
        () => console.log(`job2 complete.`)
    );   
    job2.run();
}
